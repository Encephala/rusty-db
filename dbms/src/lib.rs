#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

mod table;
mod types;
mod execute;

use types::ColumnName;
use sql_parse::{Statement, Expression, ColumnType, InfixOperator, CreateType};


pub use table::Table;
pub use execute::{Execute, RuntimeEnvironment};


pub fn execute_statement(statement: Statement, env: &mut RuntimeEnvironment) -> Result<(), SqlError> {
    return statement.execute(env);
}


#[derive(Debug)]
pub enum SqlError {
    UnequalLengths(usize, usize),
    IndexOutOfBounds(usize, usize),
    NameDoesNotExist(ColumnName, Vec<ColumnName>),
    IncompatibleTypes(Vec<ColumnType>, Vec<ColumnType>),
    ImpossibleConversion(Expression, &'static str),
    InvalidOperation(InfixOperator, &'static str, &'static str),
    ColumnNameNotUnique(ColumnName),
    InvalidParameter,
    DuplicateTable(String),
    TableDoesNotExist(String),
}
