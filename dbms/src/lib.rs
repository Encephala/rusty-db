#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

mod database;
pub mod types;
pub mod evaluate;
mod utils;
pub mod persistence;
pub mod serialisation;
pub mod server;

use types::{ColumnName, ColumnValue, TableName};
use sql_parse::parser::{ColumnType, Expression, InfixOperator};


pub use database::Database;
use types::DatabaseName;



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

    ImpossibleComparison(ColumnValue, ColumnValue),

    DuplicateTable(String),
    TableDoesNotExist(TableName),
    NoDatabaseSelected,
    DatabaseDoesNotExist(DatabaseName),

    FSError(std::io::Error),
    CouldNotStoreDatabase(DatabaseName, std::io::Error),
    CouldNotRemoveDatabase(DatabaseName, std::io::Error),
    CouldNotStoreTable(TableName, std::io::Error),
    CouldNotRemoveTable(TableName, std::io::Error),

    SliceConversionError(std::array::TryFromSliceError),
    InputTooShort(usize, usize),
    InvalidStringEncoding(std::string::FromUtf8Error),
    NotATypeDiscriminator(u8),
    NotABoolean(u8),

    IncompatibleVersion(u8),

    CouldNotWriteToConnection(std::io::Error),
    CouldNotReadFromConnection(std::io::Error),
}

pub type Result<T> = std::result::Result<T, SqlError>;
