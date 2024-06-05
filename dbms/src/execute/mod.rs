#[cfg(test)]
mod tests;

use std::collections::HashMap;

use super::{CreateType, Expression, SqlError, Statement, Table, ColumnValue};


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
        let table = self.0.get_mut(table_name);

        if let Some(table) = table {
            table.insert_multiple(values)?;

            return Ok(());
        } else {
            return Err(SqlError::TableDoesNotExist(table_name.to_string()));
        }
    }

    pub fn update(&mut self) -> Result<(), SqlError> {
        todo!();
    }

    pub fn delete(&mut self, table_name: &str, condition: Option<Expression>) -> Result<(), SqlError> {
        let table = self.0.get_mut(table_name);

        if let Some(table) = table {
            return table.delete(condition);
        } else {
            return Err(SqlError::TableDoesNotExist(table_name.to_string()));
        }
    }

    pub fn drop(&mut self, table_name: &str) -> Result<(), SqlError> {
        match self.0.remove(table_name) {
            Some(table) => Ok(()),
            None => Err(SqlError::TableDoesNotExist(table_name.to_string()))
        }
    }
}

pub trait Execute {
    fn execute(self, environment: &mut RuntimeEnvironment) -> Result<(), SqlError>;
}

// Todo: I need a better way of converting these stupid ass types
// rather than all these if let statements
impl Execute for Statement {
    fn execute(self, environment: &mut RuntimeEnvironment) -> Result<(), SqlError> {
        match self {
            Statement::Select { columns, table, where_clause } => {
                todo!();
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
                            )?);
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

                        return environment.insert(&name, result);
                    } else {
                        panic!("Tried inserting into tables but values wasn't an array");
                    }
                } else {
                    panic!("Tried inserting into table but name wasn't an Ident");
                }
            },
            Statement::Update { from, columns, values, where_clause } => {
                todo!();
            },
            Statement::Delete { from, where_clause } => {
                if let Expression::Ident(name) = from {
                    return environment.delete(&name, where_clause);
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
                            return environment.drop(&name);
                        } else {
                            panic!("Tried dropping table but name wasn't an Ident");
                        }
                    }
                }
            },
        }
    }
}
