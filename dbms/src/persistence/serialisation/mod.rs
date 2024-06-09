mod v1;

use super::SqlError;
use crate::database::{Table, RowSet};

use v1::V1;

#[derive(Debug)]
pub enum Serialiser {
    V1,
}

impl From<&Serialiser> for &[u8] {
    fn from(value: &Serialiser) -> Self {
        return match value {
            Serialiser::V1 => &[1],
        };
    }
}

impl Serialiser {
    fn serialise_table(&self, value: &Table) -> Result<Vec<u8>, SqlError> {
        return match self {
            Serialiser::V1 => V1.serialise_table(value),
        };
    }

    fn deserialise_table(&self, value: &mut &[u8]) -> Result<Table, SqlError> {
        return match self {
            Serialiser::V1 => V1.deserialise_table(value),
        };
    }
}

#[derive(Debug)]
pub struct SerialisationManager(
    Serialiser
);

impl SerialisationManager {
    pub fn new(serialiser: Serialiser) -> Self {
        return Self(serialiser);
    }

    pub fn serialise_table(&self, value: &Table) -> Result<Vec<u8>, SqlError> {
        let mut result = vec![];

        let version: &[u8] = (&self.0).into();

        result.extend(version);

        result.extend(self.0.serialise_table(value)?);

        return Ok(result);
    }

    pub fn deserialise_table(&self, mut value: &[u8]) -> Result<Table, SqlError> {
        let value = &mut value;

        if value.is_empty() {
            return Err(SqlError::InputTooShort(value.len(), 1));
        }

        let version = *value.first().unwrap();

        *value = &value[1..];

        let serialiser = match version {
            1 => Serialiser::V1,
            _ => return Err(SqlError::IncompatibleVersion(version)),
        };

        return serialiser.deserialise_table(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::utils::tests::*;

    #[test]
    fn header_parsing_basic() {
        let table = test_table();

        let manager = SerialisationManager::new(Serialiser::V1);

        let serialised = manager.serialise_table(&table).unwrap();

        assert_eq!(
            serialised.first().unwrap(),
            &1
        );

        let deserialised = manager.deserialise_table(serialised.as_slice()).unwrap();

        assert_eq!(
            deserialised,
            table
        );
    }

    #[test]
    fn header_parsing_invalid_header() {
        let table = test_table();

        let manager = SerialisationManager::new(Serialiser::V1);

        let mut serialised = manager.serialise_table(&table).unwrap();

        let first = serialised.first_mut().unwrap();

        *first = 0;

        assert!(matches!(
            manager.deserialise_table(serialised.as_slice()),
            Err(SqlError::IncompatibleVersion(0))
        ));
    }
}
