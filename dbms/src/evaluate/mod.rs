#[cfg(test)]
mod tests;

use sql_parse::{Expression, Statement, CreateType};

use super::SqlError;
use super::database::{Database, Table, RowSet};
use super::persistence::PersistenceManager;
use super::types::{DatabaseName, TableName, ColumnValue, ColumnName, Where, ColumnSelector};


impl Database {
    pub fn create(&mut self, table: Table) -> Result<(), SqlError> {
        if self.tables.contains_key(&table.name.0) {
            return Err(SqlError::DuplicateTable(table.name.0.clone()));
        }

        self.tables.insert(table.name.0.clone(), table);

        return Ok(());
    }

    pub fn insert(&mut self, table_name: TableName, columns: Option<Vec<ColumnName>>, values: Vec<Vec<ColumnValue>>) -> Result<(), SqlError> {
        let table = self.tables.get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.insert_multiple(&columns, values);
    }

    pub fn select(&self, table_name: TableName, columns: ColumnSelector, condition: Option<Where>) -> Result<RowSet, SqlError> {
        let table = self.tables.get(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.select(columns, condition);
    }

    pub fn update(&mut self,
        table_name: TableName,
        column_names: Vec<ColumnName>,
        new_values: Vec<ColumnValue>,
        condition: Option<Where>,
    ) -> Result<(), SqlError> {
        let table = self.tables.get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.update(column_names, new_values, condition);
    }

    pub fn delete(&mut self, table_name: TableName, condition: Option<Where>) -> Result<(), SqlError> {
        let table = self.tables.get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.delete(condition);
    }

    pub fn drop(&mut self, table_name: TableName) -> Result<Table, SqlError> {
        return self.tables.remove(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name));
    }
}

#[derive(Debug)]
pub enum ExecutionResult {
    None,
    Table(Table),
    Select(RowSet),
    CreateDatabase(DatabaseName),
}

pub trait Execute {
    fn execute(self, database: Option<&mut Database>, persistence: &dyn PersistenceManager) -> Result<ExecutionResult, SqlError>;
}

// Helper to destructure Array expressions
fn try_destructure_array(input: Expression) -> Result<Vec<Expression>, SqlError> {
    return match input {
        Expression::Array(values) => Ok(values),
        _ => Err(SqlError::InvalidParameter),
    };
}

// Helper to map the option because TryFrom can't be implemented for Options
// (unless Expression had been in this crate)
fn map_option_where_clause(input: Option<Expression>) -> Result<Option<Where>, SqlError> {
    return match input {
        Some(clause) => Ok(Some(clause.try_into()?)),
        None => Ok(None),
    };
}

// TODO: This receives a database to evaluate in,
// but uhhh what about creating databases?
impl Execute for Statement {
    fn execute(self, database: Option<&mut Database>, persistence_manager: &dyn PersistenceManager) -> Result<ExecutionResult, SqlError> {
        match self {
            Statement::Select { table, columns, where_clause } => {
                if database.is_none() {
                    return Err(SqlError::NoDatabaseSelected);
                }

                let database = &mut database.unwrap();

                let table: TableName = table.try_into()?;

                let columns: ColumnSelector = columns.try_into()?;

                let where_clause = map_option_where_clause(where_clause)?;

                return database.select(table, columns, where_clause)
                    .map(ExecutionResult::Select);
            },

            Statement::Create { what, name, columns } => {
                match what {
                    CreateType::Database => {
                        let database = Database::new(name.try_into()?);

                        persistence_manager.save_database(&database)?;

                        return Ok(ExecutionResult::CreateDatabase(database.name));
                    },
                    CreateType::Table => {
                        if database.is_none() {
                            return Err(SqlError::NoDatabaseSelected);
                        }

                        let database = database.unwrap();

                        let columns = try_destructure_array(columns.ok_or(SqlError::InvalidParameter)?)?;

                        let columns = columns.into_iter()
                            .map(|column_definition| column_definition.try_into())
                            .collect::<Result<Vec<_>, SqlError>>()?;

                        return database.create(Table::new(
                            name.try_into()?,
                            columns
                        )?).map(|_| ExecutionResult::None);
                    },
                };
            },

            Statement::Insert { into, columns,  values } => {
                if database.is_none() {
                    return Err(SqlError::NoDatabaseSelected);
                }

                let database = database.unwrap();

                let into: TableName = into.try_into()?;


                let values = try_destructure_array(values)?;

                let mut result = vec![];

                for row in values {
                    let row = try_destructure_array(row)?;

                    let row_values = row.into_iter()
                        .map(|value| value.try_into())
                        .collect::<Result<Vec<_>, SqlError>>()?;

                    result.push(row_values);
                }


                let columns = match columns {
                    Some(columns) => {
                        let names = try_destructure_array(columns)?;

                        let names: Vec<ColumnName> = names.into_iter()
                            .map(|name| name.try_into())
                            .collect::<Result<Vec<_>, SqlError>>()?;

                        Some(names)
                    },
                    None => None,
                };

                return database.insert(into, columns, result)
                    .map(|_| ExecutionResult::None);
            },

            Statement::Update { from, columns, values, where_clause } => {
                if database.is_none() {
                    return Err(SqlError::NoDatabaseSelected);
                }

                let database = database.unwrap();

                let from: TableName = from.try_into()?;


                let columns = try_destructure_array(columns)?;

                let column_names = columns.into_iter()
                    .map(|column_name| column_name.try_into())
                    .collect::<Result<Vec<_>, SqlError>>()?;


                let values = try_destructure_array(values)?;

                let values = values.into_iter()
                    .map(|value| value.try_into())
                    .collect::<Result<Vec<_>, SqlError>>()?;


                let where_clause = map_option_where_clause(where_clause)?;

                return database.update(from, column_names, values, where_clause)
                    .map(|_| ExecutionResult::None);
            },

            Statement::Delete { from, where_clause } => {
                if database.is_none() {
                    return Err(SqlError::NoDatabaseSelected);
                }

                let database = database.unwrap();

                let from: TableName = from.try_into()?;

                let where_clause = map_option_where_clause(where_clause)?;

                return database.delete(from, where_clause)
                    .map(|_| ExecutionResult::None);
            },

            Statement::Drop { what, name } => {
                if database.is_none() {
                    return Err(SqlError::NoDatabaseSelected);
                }

                let database = database.unwrap();

                match what {
                    CreateType::Database => {
                        todo!();
                    },
                    CreateType::Table => {
                        let name: TableName = name.try_into()?;

                        return database.drop(name)
                            .map(ExecutionResult::Table);
                    }
                }
            },
        }
    }
}
