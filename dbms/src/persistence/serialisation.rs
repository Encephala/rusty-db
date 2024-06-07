use sql_parse::ColumnType;

use super::SqlError;
use super::super::database::{Table, Row};
use super::super::types::{TableName, ColumnName, ColumnValue};

pub trait Serialise {
    fn serialise(&self) -> Result<Vec<u8>, SqlError>;
}

pub const SIZEOF_USIZE: usize = std::mem::size_of::<usize>();

pub fn usize_to_bytes(input: usize) -> Vec<u8> {
    // https://stackoverflow.com/questions/72631065/how-to-convert-a-u32-array-to-a-u8-array-in-place
    let mut result = Vec::with_capacity(SIZEOF_USIZE);

    for byte in input.to_le_bytes() {
        result.push(byte)
    }

    return result;
}

impl Serialise for Table {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        let mut result = vec![];

        let name = self.name.serialise()?;

        result.extend(usize_to_bytes(name.len()));
        result.extend(name);

        let types = self.types.serialise()?;

        result.extend(usize_to_bytes(types.len()));
        result.extend(types);

        let names = self.column_names.serialise()?;

        result.extend(usize_to_bytes(names.len()));
        result.extend(names);

        let values = self.values.serialise()?;

        result.extend(usize_to_bytes(values.len()));
        result.extend(values);

        return Ok(result);
    }
}

impl Serialise for TableName {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        return Ok(self.0.bytes().collect());
    }
}

impl Serialise for ColumnType {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        return match self {
            ColumnType::Int => Ok(vec![0]),
            ColumnType::Decimal => Ok(vec![1]),
            ColumnType::Text => Ok(vec![2]),
            ColumnType::Bool => Ok(vec![3]),
        };
    }
}

impl Serialise for ColumnName {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        return Ok(self.0.bytes().collect());
    }
}

impl Serialise for Row {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        return self.0.serialise();
    }
}

impl Serialise for ColumnValue {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        return match self {
            ColumnValue::Int(value) => Ok(usize_to_bytes(*value)),
            ColumnValue::Decimal(whole, fractional) => {
                let mut result = usize_to_bytes(*whole);

                result.append(&mut usize_to_bytes(*fractional));

                Ok(result)
            },
            ColumnValue::Str(value) => Ok(value.bytes().collect()),
            ColumnValue::Bool(value) => Ok(vec![*value as u8]),
        }
    }
}

impl<T> Serialise for Vec<T>
where T: Serialise {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        let mut result = Vec::new();

        for t in self {
            let mut bytes = t.serialise()?;

            result.append(&mut bytes);
        }

        return Ok(result);
    }
}

pub trait Deserialise {
    type Result;

    fn deserialise(&self) -> Result<Self::Result, SqlError>;
}
