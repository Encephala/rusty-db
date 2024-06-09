mod v1;

use super::SqlError;
use crate::database::Table;

pub use v1::V1;

pub trait Serialiser {
    fn serialise_table(&self, value: &Table) -> Result<Vec<u8>, SqlError>;

    fn deserialise_table(&self, value: &mut &[u8]) -> Result<Table, SqlError>;
}
