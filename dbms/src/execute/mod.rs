#[cfg(test)]
mod tests;

use std::collections::HashMap;

use sql_parse::CreateType;
use super::{Expression, SqlError, Statement, Table, Row};
use super::types::{TableName, ColumnValue, ColumnName, Where, ColumnSelector};


#[derive(Debug, Default)]
pub struct RuntimeEnvironment(pub HashMap<String, Table>);
impl RuntimeEnvironment {
    pub fn new() -> Self {
        return Self(HashMap::new());
    }

    pub fn create(&mut self, table: Table) -> Result<(), SqlError> {
        if self.0.contains_key(&table.name.0) {
            return Err(SqlError::DuplicateTable(table.name.0.clone()));
        }

        self.0.insert(table.name.0.clone(), table);

        return Ok(());
    }

    pub fn insert(&mut self, table_name: TableName, columns: Option<Vec<ColumnName>>, values: Vec<Vec<ColumnValue>>) -> Result<(), SqlError> {
        let table = self.0.get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.insert_multiple(&columns, values);
    }

    pub fn select(&mut self, table_name: TableName, columns: ColumnSelector, condition: Option<Where>) -> Result<Vec<Row>, SqlError> {
        let table = self.0.get(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.select(columns, condition);
    }

    pub fn update(&mut self,
        table_name: TableName,
        column_names: Vec<ColumnName>,
        new_values: Vec<ColumnValue>,
        condition: Option<Where>,
    ) -> Result<(), SqlError> {
        let table = self.0.get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.update(column_names, new_values, &condition);
    }

    pub fn delete(&mut self, table_name: TableName, condition: Option<Where>) -> Result<(), SqlError> {
        let table = self.0.get_mut(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name))?;

        return table.delete(condition);
    }

    pub fn drop(&mut self, table_name: TableName) -> Result<Table, SqlError> {
        return self.0.remove(&table_name.0)
            .ok_or(SqlError::TableDoesNotExist(table_name));
    }
}

#[derive(Debug)]
pub enum ExecutionResult {
    None,
    Table(Table),
    Select(Vec<Row>),
}

pub trait Execute {
    fn execute(self, environment: &mut RuntimeEnvironment) -> Result<ExecutionResult, SqlError>;
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

// Todo: I need a better way of converting these stupid ass types
// rather than all these if let statements
impl Execute for Statement {
    fn execute(self, environment: &mut RuntimeEnvironment) -> Result<ExecutionResult, SqlError> {
        match self {
            Statement::Select { table, columns, where_clause } => {
                let table: TableName = table.try_into()?;

                let columns: ColumnSelector = columns.try_into()?;

                let where_clause = map_option_where_clause(where_clause)?;

                return environment.select(table, columns, where_clause)
                    .map(ExecutionResult::Select);
            },

            Statement::Create { what, name, columns } => {
                match what {
                    CreateType::Database => {
                        todo!();
                    },
                    CreateType::Table => {
                        let columns = try_destructure_array(columns.ok_or(SqlError::InvalidParameter)?)?;

                        let columns = columns.into_iter()
                            .map(|column_definition| column_definition.try_into())
                            .collect::<Result<Vec<_>, SqlError>>()?;

                        return environment.create(Table::new(
                            name.try_into()?,
                            columns
                        )?).map(|_| ExecutionResult::None);
                    },
                };
            },

            Statement::Insert { into, columns,  values } => {
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

                return environment.insert(into, columns, result)
                    .map(|_| ExecutionResult::None);
            },

            Statement::Update { from, columns, values, where_clause } => {
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

                return environment.update(from, column_names, values, where_clause)
                    .map(|_| ExecutionResult::None);
            },

            Statement::Delete { from, where_clause } => {
                let from: TableName = from.try_into()?;

                let where_clause = map_option_where_clause(where_clause)?;

                return environment.delete(from, where_clause)
                    .map(|_| ExecutionResult::None);
            },

            Statement::Drop { what, name } => {
                match what {
                    CreateType::Database => {
                        todo!();
                    },
                    CreateType::Table => {
                        let name: TableName = name.try_into()?;

                        return environment.drop(name)
                            .map(ExecutionResult::Table);
                    }
                }
            },
        }
    }
}
