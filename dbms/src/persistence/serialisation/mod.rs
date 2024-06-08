mod v1;
#[cfg(test)]
mod v1_tests;

use super::SqlError;
use crate::ColumnType;

pub trait Serialise: std::fmt::Debug {
    fn serialise(&self) -> Result<Vec<u8>, SqlError>;
}


#[derive(Debug, Clone)]
pub enum DeserialisationOptions {
    None,
    ColumnType(ColumnType),
    ColumnTypes(Vec<ColumnType>),
}
use DeserialisationOptions as DO;

// Aren't I a clever one
impl From<Option<DeserialisationOptions>> for DeserialisationOptions {
    fn from(value: Option<DeserialisationOptions>) -> Self {
        return match value {
            Some(params) => params,
            None => DO::None,
        }
    }
}

pub trait Deserialise {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self, SqlError> where Self: Sized;
}
