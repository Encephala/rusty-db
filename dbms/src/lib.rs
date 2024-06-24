#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

mod database;
pub mod evaluate;
pub mod persistence;
pub mod serialisation;
pub mod server;
pub mod types;
pub mod utils;

use std::ffi::OsString;

use sql_parse::parser::{ColumnType, Expression, InfixOperator};
use types::DatabaseName;
use types::{ColumnName, ColumnValue, TableName};

pub use database::Database;

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

    DuplicateDatabase(DatabaseName),
    DuplicateTable(String),
    TableDoesNotExist(TableName),
    NoDatabaseSelected,
    DatabaseDoesNotExist(DatabaseName),

    FSError(std::io::Error),
    CouldNotStoreDatabase(DatabaseName, std::io::Error),
    CouldNotRemoveDatabase(DatabaseName, std::io::Error),
    CouldNotStoreTable(TableName, std::io::Error),
    CouldNotReadTable(OsString),
    CouldNotRemoveTable(TableName, std::io::Error),

    SliceConversionError(std::array::TryFromSliceError),
    InputTooShort(usize, usize),
    NotAValidString(std::string::FromUtf8Error),
    NotATypeDiscriminator(u8),
    NotABoolean(u8),

    IncompatibleVersion(u8),

    InvalidHeader(&'static str),
    InvalidMessageType(u8),
    InvalidMessage(Vec<u8>),

    CouldNotWriteToConnection(std::io::Error),
    CouldNotReadFromConnection(std::io::Error),

    ParseError,
    InvalidCommand(String),
}

pub type Result<T> = std::result::Result<T, SqlError>;
