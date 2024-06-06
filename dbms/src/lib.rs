#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

mod table;
mod types;
mod execute;
mod utils;

use types::{ColumnName, TableName};
use sql_parse::{Statement, Expression, CreateType};
pub use sql_parse::{InfixOperator, ColumnType};


use table::{Table, Row};
pub use execute::{Execute, RuntimeEnvironment, ExecutionResult};


pub fn execute_statement(statement: Statement, env: &mut RuntimeEnvironment) -> Result<ExecutionResult, SqlError> {
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
    TableDoesNotExist(TableName),
}
