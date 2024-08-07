#[cfg(test)]
mod tests;

use sql_parse::parser::{CreateType, Expression, Statement};

use super::database::{Database, RowSet, Table};
use super::types::{ColumnName, ColumnSelector, ColumnValue, DatabaseName, TableName, Where};
use super::SqlError;
use crate::server::Runtime;
use crate::types::{ColumnDefinition, ForeignKeyConstraint};
use crate::Result;

impl Database {
    pub fn create(&mut self, table: Table) -> Result<()> {
        if self.tables.contains_key(&table.schema.name.0) {
            return Err(SqlError::DuplicateTable(table.schema.name.0.clone()));
        }

        self.tables.insert(table.schema.name.0.clone(), table);

        return Ok(());
    }

    pub fn insert(
        &mut self,
        table_name: TableName,
        columns: Option<Vec<ColumnName>>,
        values: Vec<Vec<ColumnValue>>,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.insert_multiple(&columns, values);
    }

    pub fn select(
        &self,
        table_name: TableName,
        columns: ColumnSelector,
        condition: Option<Where>,
    ) -> Result<RowSet> {
        let table = self
            .tables
            .get(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.select(columns, condition);
    }

    pub fn update(
        &mut self,
        table_name: TableName,
        column_names: Vec<ColumnName>,
        new_values: Vec<ColumnValue>,
        condition: Option<Where>,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.update(column_names, new_values, condition);
    }

    pub fn delete(&mut self, table_name: TableName, condition: Option<Where>) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.delete(condition);
    }

    pub fn drop_table(&mut self, table_name: TableName) -> Result<Table> {
        return self
            .tables
            .remove(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name));
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum ExecutionResult {
    None,
    Table(Table),
    Select(RowSet),
    CreateDatabase(DatabaseName),
    DropDatabase(DatabaseName),
    ListTables(Vec<String>),
}

impl From<Option<ExecutionResult>> for ExecutionResult {
    fn from(value: Option<ExecutionResult>) -> Self {
        return match value {
            Some(result) => result,
            None => ExecutionResult::None,
        };
    }
}

// Because Statement is a foreign type
pub trait Execute {
    fn execute(
        &self,
        runtime: &mut Runtime,
    ) -> impl futures::Future<Output = Result<ExecutionResult>> + Send;
}

// Helper to destructure Array expressions
fn try_destructure_array(input: &Expression) -> Result<&Vec<Expression>> {
    return match input {
        Expression::Array(values) => Ok(values),
        _ => Err(SqlError::InvalidParameter),
    };
}

// Helper to map the option because TryFrom can't be implemented for Options
// (unless Expression had been in this crate)
fn map_option_where_clause(input: &Option<Expression>) -> Result<Option<Where>> {
    return match input {
        Some(clause) => Ok(Some(clause.try_into()?)),
        None => Ok(None),
    };
}

impl Execute for Statement {
    async fn execute(&self, runtime: &mut Runtime) -> Result<ExecutionResult> {
        let result = execute_statement(self, runtime).await;

        // I guess an error could mean invalid state, so don't persist an invalid state
        // I could go through all the ways this could fail and check if it actually leaves invalid states,
        // but ykno
        // I guess not saving still leaves it in the old invalid state in memory, should refresh that or something
        // TODO
        if result.is_ok() {
            runtime.save().await?;
        }

        return result;
    }
}

async fn execute_statement(
    statement: &Statement,
    runtime: &mut Runtime,
) -> Result<ExecutionResult> {
    let database = runtime.get_database();

    match statement {
        Statement::Select {
            table,
            columns,
            where_clause,
        } => {
            if database.is_none() {
                return Err(SqlError::NoDatabaseSelected);
            }

            let database = database.unwrap();

            let table: TableName = table.try_into()?;

            let columns: ColumnSelector = columns.try_into()?;

            let where_clause = map_option_where_clause(where_clause)?;

            return database
                .select(table, columns, where_clause)
                .map(ExecutionResult::Select);
        }

        Statement::Create {
            what,
            name,
            columns,
        } => {
            match what {
                CreateType::Database => {
                    let database = Database::new(name.try_into()?);

                    let name = database.name.clone();

                    runtime.create_database(database);

                    return Ok(ExecutionResult::CreateDatabase(name));
                }
                CreateType::Table => {
                    if database.is_none() {
                        return Err(SqlError::NoDatabaseSelected);
                    }

                    let database = database.unwrap();

                    let columns =
                        try_destructure_array(columns.as_ref().ok_or(SqlError::InvalidParameter)?)?;

                    let mut column_definitions = vec![];
                    let mut constraints = vec![];

                    for column in columns {
                        let column_definition: Result<ColumnDefinition> = column.try_into();
                        if let Ok(column_definition) = column_definition {
                            column_definitions.push(column_definition);

                            continue;
                        }

                        let constraint: Result<ForeignKeyConstraint> = column.try_into();
                        if let Ok(constraint) = constraint {
                            constraints.push(constraint);

                            continue;
                        }

                        return Err(SqlError::ImpossibleConversion(column.clone(), "A column definiton or foreign key constraint"))
                    }

                    return database
                        .create(Table::new(name.try_into()?, column_definitions, constraints)?)
                        .map(|_| ExecutionResult::None);
                }
            };
        }

        Statement::Insert {
            into,
            columns,
            values,
        } => {
            if database.is_none() {
                return Err(SqlError::NoDatabaseSelected);
            }

            let database = database.unwrap();

            let into = TableName::try_from(into)?;

            let values = try_destructure_array(values)?;

            let mut result = vec![];

            for row in values {
                let row = try_destructure_array(row)?;

                let row_values = row
                    .iter()
                    .map(|value| value.try_into())
                    .collect::<Result<Vec<_>>>()?;

                result.push(row_values);
            }

            let columns = match columns {
                Some(columns) => {
                    let names = try_destructure_array(columns)?;

                    let names: Vec<ColumnName> = names
                        .iter()
                        .map(|name| name.try_into())
                        .collect::<Result<Vec<_>>>()?;

                    Some(names)
                }
                None => None,
            };

            return database
                .insert(into, columns, result)
                .map(|_| ExecutionResult::None);
        }

        Statement::Update {
            from,
            columns,
            values,
            where_clause,
        } => {
            if database.is_none() {
                return Err(SqlError::NoDatabaseSelected);
            }

            let database = database.unwrap();

            let from: TableName = from.try_into()?;

            let columns = try_destructure_array(columns)?;

            let column_names = columns
                .iter()
                .map(|column_name| column_name.try_into())
                .collect::<Result<Vec<_>>>()?;

            let values = try_destructure_array(values)?;

            let values = values
                .iter()
                .map(|value| value.try_into())
                .collect::<Result<Vec<_>>>()?;

            let where_clause = map_option_where_clause(where_clause)?;

            return database
                .update(from, column_names, values, where_clause)
                .map(|_| ExecutionResult::None);
        }

        Statement::Delete { from, where_clause } => {
            if database.is_none() {
                return Err(SqlError::NoDatabaseSelected);
            }

            let database = database.unwrap();

            let from: TableName = from.try_into()?;

            let where_clause = map_option_where_clause(where_clause)?;

            return database
                .delete(from, where_clause)
                .map(|_| ExecutionResult::None);
        }

        Statement::Drop { what, name } => match what {
            CreateType::Database => {
                let name = DatabaseName::try_from(name)?;

                runtime.drop().await?;

                return Ok(ExecutionResult::DropDatabase(name));
            }
            CreateType::Table => {
                if database.is_none() {
                    return Err(SqlError::NoDatabaseSelected);
                }

                let database = database.unwrap();

                let name: TableName = name.try_into()?;

                return database.drop_table(name).map(ExecutionResult::Table);
            }
        },
    }
}
