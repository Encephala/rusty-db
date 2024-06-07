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

        // TODO: This is no bueno, we don't know the size of each column
        // Could store it along with columntypes for all except TEXT because TEXT is arbitrary length fuck
        // maybe I should go uhh varchar xd
        // wait no that's variable length too so same problem, it's kinda in the name
        // Can store it with value for text I guess,
        // and just reconstruct it from column types for others
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
            ColumnValue::Str(value) => Ok(value.as_bytes().to_vec()),
            ColumnValue::Bool(value) => Ok(vec![*value as u8]),
        }
    }
}

impl<T: Serialise> Serialise for Vec<T> {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        let mut result = Vec::new();

        for t in self {
            let mut bytes = t.serialise()?;

            result.append(&mut bytes);
        }

        return Ok(result);
    }
}

pub enum DeserialisationOptions {
    None,
    Length(usize), // For TEXT (and other potential variable length types)
    // lengths of columns, ...
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
    type Result;

    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self::Result, SqlError>;
}

impl Deserialise for Table {
    type Result = Self;

    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self::Result, SqlError> {
        let name_size = usize::deserialise(input, None.into())?;

        let name = TableName::deserialise(input, DO::Length(name_size))?;

        let types_size = usize::deserialise(input, None.into())?;

        let types = Vec::<ColumnType>::deserialise(input, DO::Length(types_size))?;



        todo!();
    }
}

impl Deserialise for usize {
    type Result = usize;

    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self::Result, SqlError> {
        if input.len() < SIZEOF_USIZE {
            return Err(SqlError::InputTooShort(input.len(), SIZEOF_USIZE))
        }

        // try_into to convert length(?)
        let bytes: [u8; SIZEOF_USIZE] = input[..SIZEOF_USIZE].try_into()
            .map_err(SqlError::SliceConversionError)?;

        let result = usize::from_le_bytes(bytes);

        *input = &input[SIZEOF_USIZE..];

        return Ok(result);
    }
}

impl Deserialise for TableName {
    type Result = Self;

    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self::Result, SqlError> {
        let length = match options {
            DO::Length(length) => length,
            _ => return Err(SqlError::InvalidParameter),
        };

        if input.len() < length {
            return Err(SqlError::InputTooShort(input.len(), length));
        }

        let bytes = input[..length].to_vec();

        // https://doc.rust-lang.org/book/ch08-02-strings.html
        // strings are UTF8 in rust
        let result = String::from_utf8(bytes)
            .map_err(SqlError::InvalidStringEncoding)?;

        return Ok(TableName(result));
    }
}

impl Deserialise for ColumnType {
    type Result = Self;

    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self::Result, SqlError> {
        // A ColumnType is serialised as one byte
        if input.is_empty() {
            return Err(SqlError::InputTooShort(input.len(), 1));
        }

        let byte = input.first().unwrap();

        *input = &input[1..];

        return match byte {
            0 => Ok(ColumnType::Int),
            1 => Ok(ColumnType::Decimal),
            2 => Ok(ColumnType::Text),
            3 => Ok(ColumnType::Bool),
            _ => Err(SqlError::NotATypeDiscriminator(*byte)),
        };
    }
}

impl Deserialise for Vec<ColumnType> {
    type Result = Self;

    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self::Result, SqlError> {
        let length = match options {
            DO::Length(length) => length,
            _ => return Err(SqlError::InvalidParameter),
        };

        let mut result = vec![];

        for _ in 0..length {
            result.push(
                ColumnType::deserialise(input, None.into())?
            );
        }

        return Ok(result);
    }
}
