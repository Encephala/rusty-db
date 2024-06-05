#[cfg(test)]
mod tests;

use std::collections::HashMap;

use super::{CreateType, Expression, SqlError, Statement, Table, Row, ColumnValue, ColumnName, ColumnSelector};


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

    pub fn insert(&mut self, table_name: &str, values: Vec<Vec<ColumnValue>>) -> Result<(), SqlError> {
        let table = self.0.get_mut(table_name)
            .ok_or(SqlError::TableDoesNotExist(table_name.to_string()))?;

        return table.insert_multiple(values);
    }

    pub fn select(&mut self, table_name: &str, columns: ColumnSelector, condition: Option<Expression>) -> Result<Vec<Row>, SqlError> {
        let table = self.0.get(table_name)
            .ok_or(SqlError::TableDoesNotExist(table_name.into()))?;

        return table.select(columns, condition);
    }

    pub fn update(&mut self,
        table_name: &str,
        column_names: Vec<ColumnName>,
        new_values: Vec<ColumnValue>,
        condition: Option<Expression>,
    ) -> Result<(), SqlError> {
        let table = self.0.get_mut(table_name)
            .ok_or(SqlError::TableDoesNotExist(table_name.to_string()))?;

        return table.update(column_names, new_values, condition);
    }

    pub fn delete(&mut self, table_name: &str, condition: Option<Expression>) -> Result<(), SqlError> {
        let table = self.0.get_mut(table_name)
            .ok_or(SqlError::TableDoesNotExist(table_name.to_string()))?;

        return table.delete(condition);
    }

    pub fn drop(&mut self, table_name: &str) -> Result<Table, SqlError> {
        return self.0.remove(table_name)
            .ok_or(SqlError::TableDoesNotExist(table_name.to_string()));
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

// Todo: I need a better way of converting these stupid ass types
// rather than all these if let statements
impl Execute for Statement {
    fn execute(self, environment: &mut RuntimeEnvironment) -> Result<ExecutionResult, SqlError> {
        match self {
            Statement::Select { table, columns, where_clause } => {
                if let Expression::Ident(table) = table {
                    let columns = match columns {
                        Expression::Array(columns) => {
                            let mut column_names = vec![];

                            for column in columns {
                                if let Expression::Ident(column_name) = column {
                                    column_names.push(ColumnName(column_name));
                                } else {
                                    panic!("Tried selecting columns but column wasn't an Ident");
                                }
                            }

                            ColumnSelector::Name(column_names)
                        },
                        Expression::AllColumns => {
                            ColumnSelector::AllColumns
                        },
                        _ => panic!("Tried selecting from table but columns wasn't an Array")
                    };

                    return environment.select(&table, columns, where_clause)
                        .map(ExecutionResult::Select);
                } else {
                    panic!("Tried selecting from table but table name wasn't an Ident");
                }
            },
            Statement::Create { what, name, columns } => {
                match what {
                    CreateType::Database => {
                        todo!();
                    },
                    CreateType::Table => {
                        if let Expression::Array(columns) = columns.ok_or(SqlError::InvalidParameter)? {
                            return environment.create(Table::new(
                                name,
                                columns
                            )?).map(|_| ExecutionResult::None);
                        } else {
                            panic!("Tried creating table but columns wasn't an Array");
                        }
                    },
                };
            },
            Statement::Insert { into, values } => {
                if let Expression::Ident(name) = into {
                    if let Expression::Array(values) = values {
                        let mut result = vec![];

                        for row in values {
                            if let Expression::Array(row) = row {
                                let mut row_values = vec![];

                                for value in row {
                                    row_values.push(value.try_into()?);
                                }

                                result.push(row_values);
                            } else {
                                panic!("Tried inserting into tables but values wasn't an array")
                            }
                        }

                        return environment.insert(&name, result)
                            .map(|_| ExecutionResult::None);
                    } else {
                        panic!("Tried inserting into tables but values wasn't an array");
                    }
                } else {
                    panic!("Tried inserting into table but name wasn't an Ident");
                }
            },
            Statement::Update { from, columns, values, where_clause } => {
                if let Expression::Ident(from) = from {
                    if let Expression::Array(columns) = columns {
                        let mut column_names = vec![];

                        for column in columns {
                            if let Expression::Ident(column_name) = column {
                                column_names.push(ColumnName(column_name));
                            } else {
                                panic!("Tried updating table but column name wasn't an Ident")
                            }
                        }

                        if let Expression::Array(values) = values {
                            let mut column_values = vec![];

                            for value in values {
                                column_values.push(value.try_into()?);
                            }

                            return environment.update(&from, column_names, column_values, where_clause)
                                .map(|_| ExecutionResult::None);
                        }
                    } else {
                        panic!("Tried updating table but columns wasn't an Array");
                    }
                } else {
                    panic!("Tried updating table but name wasn't an Ident");
                }

                todo!();
            },
            Statement::Delete { from, where_clause } => {
                if let Expression::Ident(name) = from {
                    return environment.delete(&name, where_clause)
                        .map(|_| ExecutionResult::None);
                } else {
                    panic!("Tried deleting from table but name wasn't an Ident");
                }
            },
            Statement::Drop { what, name } => {
                match what {
                    CreateType::Database => {
                        todo!();
                    },
                    CreateType::Table => {
                        if let Expression::Ident(name) = name {
                            return environment.drop(&name)
                                .map(ExecutionResult::Table);
                        } else {
                            panic!("Tried dropping table but name wasn't an Ident");
                        }
                    }
                }
            },
        }
    }
}
