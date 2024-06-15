mod v1;
mod v2;

use super::SqlError;
use crate::{
    Result,
    database::{Table, RowSet}
};

use v1::V1;
use v2::V2;

#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq))]
pub enum Serialiser {
    V1,
    V2,
}

impl std::fmt::Display for Serialiser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Serialiser::V1 => write!(f, "V1"),
            Serialiser::V2 => write!(f, "V2"),
        }
    }
}

impl From<Serialiser> for &[u8] {
    fn from(value: Serialiser) -> Self {
        return match value {
            Serialiser::V1 => &[1],
            Serialiser::V2 => &[2],
        };
    }
}

impl From<&Serialiser> for Box<dyn Serialise> {
    fn from(value: &Serialiser) -> Self {
        return match value {
            Serialiser::V1 => Box::new(V1),
            Serialiser::V2 => Box::new(V2),
        };
    }
}

impl TryFrom<u8> for Serialiser {
    type Error = SqlError;

    fn try_from(value: u8) -> Result<Self> {
        return match value {
            1 => Ok(Serialiser::V1),
            2 => Ok(Serialiser::V2),
            _ => Err(SqlError::IncompatibleVersion(value)),
        }
    }
}

trait Serialise {
    fn serialise_table(&self, value: &Table) -> Vec<u8>;

    fn serialise_rowset(&self, value: &RowSet) -> Vec<u8>;

    fn deserialise_table(&self, input: &mut &[u8]) -> Result<Table>;

    fn deserialise_rowset(&self, input: &mut &[u8]) -> Result<RowSet>;
}

impl Serialise for Serialiser {
    fn serialise_table(&self, value: &Table) -> Vec<u8> {
        let implementation: Box<dyn Serialise> = self.into();

        return implementation.serialise_table(value);
    }

    fn serialise_rowset(&self, value: &RowSet) -> Vec<u8> {
        let implementation: Box<dyn Serialise> = self.into();

        return implementation.serialise_rowset(value);
    }

    fn deserialise_table(&self, input: &mut &[u8]) -> Result<Table> {
        let implementation: Box<dyn Serialise> = self.into();

        return implementation.deserialise_table(input);
    }

    fn deserialise_rowset(&self, input: &mut &[u8]) -> Result<RowSet> {
        let implementation: Box<dyn Serialise> = self.into();

        return implementation.deserialise_rowset(input);
    }
}

#[derive(Debug)]
pub struct SerialisationManager(
    Serialiser
);

impl SerialisationManager {
    pub const fn new(serialiser: Serialiser) -> Self {
        return Self(serialiser);
    }

    fn write_version(&self) -> Vec<u8> {
        let mut result = vec![];

        let version: &[u8] = self.0.into();

        result.extend(version);

        return result;
    }

    pub fn serialise_table(&self, value: &Table) -> Vec<u8> {
        let mut result = self.write_version();

        result.extend(self.0.serialise_table(value));

        return result;
    }

    pub fn serialise_rowset(&self, value: &RowSet) -> Vec<u8> {
        let mut result = self.write_version();

        result.extend(self.0.serialise_rowset(value));

        return result;
    }

    fn read_version(&self, input: &mut &[u8]) -> Result<Serialiser> {
        if input.is_empty() {
            return Err(SqlError::InputTooShort(input.len(), 1));
        }

        let version = *input.first().unwrap();

        *input = &input[1..];

        return version.try_into();
    }

    pub fn deserialise_table(&self, mut input: &[u8]) -> Result<Table> {
        let input = &mut input;

        let serialiser = self.read_version(input)?;

        return serialiser.deserialise_table(input);
    }

    pub fn deserialise_rowset(&self, mut input: &[u8]) -> Result<RowSet> {
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

        let serialised = manager.serialise_table(&table);

        assert_eq!(
            serialised.first().unwrap(),
            &1
        );

        let rowset = table.select(
            crate::types::ColumnSelector::AllColumns,
            None
        ).unwrap();

        let serialised = manager.serialise_rowset(&rowset);

        assert_eq!(
            serialised.first().unwrap(),
            &1
        );
    }

    #[test]
    fn version_parsing_invalid_header() {
        let table = test_table();

        let manager = SerialisationManager::new(Serialiser::V1);

        let mut serialised = manager.serialise_table(&table);

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

        let mut serialised = manager.serialise_rowset(&rowset);

        let first = serialised.first_mut().unwrap();

        *first = 0;

        assert!(matches!(
            manager.deserialise_table(serialised.as_slice()),
            Err(SqlError::IncompatibleVersion(0))
        ));
    }
}
