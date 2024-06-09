mod v1;

use super::SqlError;
use crate::database::{Table, RowSet};

use v1::V1;

#[derive(Debug, Clone, Copy)]
pub enum Serialiser {
    V1,
}

impl From<Serialiser> for &[u8] {
    fn from(value: Serialiser) -> Self {
        return match value {
            Serialiser::V1 => &[1],
        };
    }
}

impl TryFrom<u8> for Serialiser {
    type Error = SqlError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        return match value {
            1 => Ok(Serialiser::V1),
            _ => Err(SqlError::IncompatibleVersion(value)),
        }
    }
}

trait Serialise {
    fn serialise_table(&self, value: &Table) -> Result<Vec<u8>, SqlError>;

    fn serialise_rowset(&self, value: &RowSet) -> Result<Vec<u8>, SqlError>;

    fn deserialise_table(&self, input: &mut &[u8]) -> Result<Table, SqlError>;

    fn deserialise_rowset(&self, input: &mut &[u8]) -> Result<RowSet, SqlError>;
}

impl Serialise for Serialiser {
    fn serialise_table(&self, value: &Table) -> Result<Vec<u8>, SqlError> {
        return match self {
            Serialiser::V1 => V1.serialise_table(value),
        };
    }

    fn serialise_rowset(&self, value: &RowSet) -> Result<Vec<u8>, SqlError> {
        return match self {
            Serialiser::V1 => V1.serialise_rowset(value),
        };
    }

    fn deserialise_table(&self, input: &mut &[u8]) -> Result<Table, SqlError> {
        return match self {
            Serialiser::V1 => V1.deserialise_table(input),
        };
    }

    fn deserialise_rowset(&self, input: &mut &[u8]) -> Result<RowSet, SqlError> {
        return match self {
            Serialiser::V1 => V1.deserialise_rowset(input),
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

    fn write_version(&self) -> Vec<u8> {
        let mut result = vec![];

        let version: &[u8] = self.0.into();

        result.extend(version);

        return result;
    }

    pub fn serialise_table(&self, value: &Table) -> Result<Vec<u8>, SqlError> {
        let mut result = self.write_version();

        result.extend(self.0.serialise_table(value)?);

        return Ok(result);
    }

    pub fn serialise_rowset(&self, value: &RowSet) -> Result<Vec<u8>, SqlError> {
        let mut result = self.write_version();

        result.extend(self.0.serialise_rowset(value)?);

        return Ok(result);
    }

    fn read_version(&self, input: &mut &[u8]) -> Result<Serialiser, SqlError> {
        if input.is_empty() {
            return Err(SqlError::InputTooShort(input.len(), 1));
        }

        let version = *input.first().unwrap();

        *input = &input[1..];

        return version.try_into();
    }

    pub fn deserialise_table(&self, mut input: &[u8]) -> Result<Table, SqlError> {
        let input = &mut input;

        let serialiser = self.read_version(input)?;

        return serialiser.deserialise_table(input);
    }

    pub fn deserialise_rowset(&self, mut input: &[u8]) -> Result<RowSet, SqlError> {
        let input = &mut input;

        let serialiser = self.read_version(input)?;

        return serialiser.deserialise_rowset(input);
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::utils::tests::*;

    #[test]
    fn version_parsing_basic() {
        let table = test_table();

        let manager = SerialisationManager::new(Serialiser::V1);

        let serialised = manager.serialise_table(&table).unwrap();

        assert_eq!(
            serialised.first().unwrap(),
            &1
        );

        let rowset = table.select(
            crate::types::ColumnSelector::AllColumns,
            None
        ).unwrap();

        let serialised = manager.serialise_rowset(&rowset).unwrap();

        assert_eq!(
            serialised.first().unwrap(),
            &1
        );
    }

    #[test]
    fn version_parsing_invalid_header() {
        let table = test_table();

        let manager = SerialisationManager::new(Serialiser::V1);

        let mut serialised = manager.serialise_table(&table).unwrap();

        let first = serialised.first_mut().unwrap();

        *first = 0;

        assert!(matches!(
            manager.deserialise_table(serialised.as_slice()),
            Err(SqlError::IncompatibleVersion(0))
        ));

        let rowset = table.select(
            crate::types::ColumnSelector::AllColumns,
            None
        ).unwrap();

        let mut serialised = manager.serialise_rowset(&rowset).unwrap();

        let first = serialised.first_mut().unwrap();

        *first = 0;

        assert!(matches!(
            manager.deserialise_table(serialised.as_slice()),
            Err(SqlError::IncompatibleVersion(0))
        ));
    }
}
