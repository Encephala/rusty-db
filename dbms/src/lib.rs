#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

mod database;
mod types;
mod evaluate;
mod utils;
mod persistence;

use types::{ColumnName, TableName};
use sql_parse::{ColumnType, Expression, InfixOperator};


pub use database::Database;
pub use types::DatabaseName;
pub use evaluate::{Execute, ExecutionResult};
pub use persistence::{PersistenceManager, FileSystem, SerialisationManager, Serialiser};



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
}

pub type Result<T> = std::result::Result<T, SqlError>;
