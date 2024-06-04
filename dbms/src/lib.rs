#![warn(missing_debug_implementations)]
#![allow(clippy::needless_return)]

use sql_parse::{Expression, ColumnType};


mod table;

#[derive(Debug)]
pub enum SqlError {
    IncompatibleTypes(Vec<ColumnType>, Vec<ColumnType>),
}
