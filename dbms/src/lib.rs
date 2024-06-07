#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

mod database;
mod types;
mod evaluate;
mod utils;
mod persistence;

use types::{ColumnName, TableName};
// TODO: remove this when repl is implemented properly
pub use types::DatabaseName;
use sql_parse::{ColumnType, Expression, InfixOperator, Statement};


pub use database::Database;
pub use evaluate::{Execute, ExecutionResult};


pub fn execute_statement(statement: Statement, database: &mut Database) -> Result<ExecutionResult, SqlError> {
    return statement.execute(database);
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
    CouldNotStoreDatabase(DatabaseName, std::io::Error),
    CouldNotRemoveDatabase(DatabaseName, std::io::Error),
    CouldNotStoreTable(TableName, std::io::Error),
    CouldNotRemoveTable(TableName, std::io::Error),
    SliceConversionError(std::array::TryFromSliceError),
    InputTooShort(usize, usize),
    InvalidStringEncoding(std::string::FromUtf8Error),
    NotATypeDiscriminator(u8),
}
