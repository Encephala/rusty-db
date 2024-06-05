#[cfg(test)]
mod tests;

use std::collections::HashMap;

use super::{Statement, Expression, SqlError};
use super::{Table, CreateType};


#[derive(Debug, Default)]
pub struct RuntimeEnvironment(pub HashMap<String, Table>);
impl RuntimeEnvironment {
    pub fn new() -> Self {
        return Self(HashMap::new());
    }

    pub fn insert(&mut self, table: Table) -> Result<(), SqlError> {
        if self.0.contains_key(&table.name.0) {
            return Err(SqlError::DuplicateTable(table.name.0.clone()));
        }

        self.0.insert(table.name.0.clone(), table);

        return Ok(());
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
                            return environment.insert(Table::new(
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
                todo!();
            },
            Statement::Update { from, columns, values, where_clause } => {
                todo!();
            },
            Statement::Delete { from, where_clause } => {
                todo!();
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
