#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

use types::ColumnName;
use sql_parse::{Expression, ColumnType, InfixOperator};

mod table;
mod types;
#[cfg(test)]
mod tests_table;


pub use table::Table;


#[derive(Debug)]
pub enum SqlError {
    UnequalLengths(usize, usize),
    IndexOutOfBounds(usize, usize),
    NameDoesNotExist(ColumnName, Vec<ColumnName>),
    IncompatibleTypes(Vec<ColumnType>, Vec<ColumnType>),
    ImpossibleConversion(Expression, &'static str),
    InvalidOperation(InfixOperator, &'static str, &'static str),
    NameNotUnique(ColumnName),
}
