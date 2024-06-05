#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

use sql_parse::{Expression, ColumnType, InfixOperator};

mod table;
mod conversions;


pub use table::Table;


#[derive(Debug)]
pub enum SqlError {
    UnequalLengths(usize, usize),
    IndexOutOfBounds(usize, usize),
    NameDoesNotExist(String, Vec<String>),
    IncompatibleTypes(Vec<ColumnType>, Vec<ColumnType>),
    ImpossibleConversion(Expression, &'static str),
    InvalidOperation(InfixOperator, &'static str, &'static str),
}
