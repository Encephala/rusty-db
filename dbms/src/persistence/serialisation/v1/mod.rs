#[cfg(test)]
mod tests;

use sql_parse::ColumnType;

use crate::SqlError;

use crate::database::{Table, Row};
use crate::types::{TableName, ColumnName, ColumnValue};
use super::Serialiser;

#[derive(Debug)]
pub struct V1;

impl Serialiser for V1 {
    fn serialise_table(&self, value: &Table) -> Result<Vec<u8>, SqlError> {
        return Table::serialise(value);
    }

    fn deserialise_table(&self, value: &mut &[u8]) -> Result<Table, SqlError> {
        return Table::deserialise(value, None.into());
    }
}

#[derive(Clone)]
enum DeserialisationOptions {
    None,
    ColumnType(ColumnType),
    ColumnTypes(Vec<ColumnType>),
}
use DeserialisationOptions as DO;

impl From<Option<DO>> for DO {
    fn from(value: Option<DO>) -> Self {
        return match value {
            Some(option) => option,
            None => DO::None,
        };
    }
}


trait Serialise {
    fn serialise(&self) -> Result<Vec<u8>, SqlError>;
}

trait Deserialise {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self, SqlError> where Self: Sized;
}

pub const SIZEOF_USIZE: usize = std::mem::size_of::<usize>();

fn usize_to_bytes(input: usize) -> Vec<u8> {
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

        result.extend(name);


        let types = self.types.serialise()?;

        result.extend(types);


        let names = self.column_names.serialise()?;

        result.extend(names);


        let values = self.values.serialise()?;

        result.extend(values);

        return Ok(result);
    }
}

impl Serialise for TableName {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        let mut result = usize_to_bytes(self.0.len());

        result.extend(self.0.bytes());

        return Ok(result);
    }
}

impl Serialise for ColumnType {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        // Start counting at 1 to make sure uninitialised data isn't a valid type
        // (for what it's worth)
        return match self {
            ColumnType::Int => Ok(vec![1]),
            ColumnType::Decimal => Ok(vec![2]),
            ColumnType::Text => Ok(vec![3]),
            ColumnType::Bool => Ok(vec![4]),
        };
    }
}

impl Serialise for ColumnName {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        let mut result = usize_to_bytes(self.0.len());

        result.extend(self.0.bytes());

        return Ok(result);
    }
}

impl Serialise for Row {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        return self.0.serialise();
    }
}

// Sure would be nice if negative impl was stable
// Then I could make a custom impl for Vec<Row> that stored the types once,
// Removing the need for DeserialisationOptions altogether.
// But I'm not about to write four different implementations
// I mean I guess I could idk
//
// Also we're storing number of values in each Vec<ColumnValue>, which isn't necessary
// because we store the number of types already, can reuse that value technically
// Ah well that's complexity anyway

impl Serialise for ColumnValue {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        return match self {
            ColumnValue::Int(value) => Ok(usize_to_bytes(*value)),
            ColumnValue::Decimal(whole, fractional) => {
                let mut result = usize_to_bytes(*whole);

                result.extend(usize_to_bytes(*fractional));

                Ok(result)
            },
            ColumnValue::Str(value) => {
                let mut result = usize_to_bytes(value.len());

                result.extend(value.as_bytes());

                Ok(result)
            },
            ColumnValue::Bool(value) => Ok(vec![*value as u8]),
        }
    }
}

impl<T: Serialise> Serialise for Vec<T> {
    fn serialise(&self) -> Result<Vec<u8>, SqlError> {
        let mut result = vec![];

        // First store total count
        result.extend(usize_to_bytes(self.len()));

        for t in self {
            let bytes = t.serialise()?;

            result.extend(bytes);
        }

        return Ok(result);
    }
}

impl Deserialise for Table {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        let name = TableName::deserialise(input, None.into())?;

        let types = Vec::<ColumnType>::deserialise(input, None.into())?;

        let column_names = Vec::<ColumnName>::deserialise(input, None.into())?;

        let values = Vec::<Row>::deserialise(input, DO::ColumnTypes(types.clone()))?;

        return Ok(Table {
            name,
            types,
            column_names,
            values,
        })
    }
}

impl Deserialise for usize {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
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
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        let length = usize::deserialise(input, None.into())?;

        let bytes = input[..length].to_vec();

        *input = &input[length..];

        // https://doc.rust-lang.org/book/ch08-02-strings.html
        // strings are UTF8 in rust
        let result = String::from_utf8(bytes)
            .map_err(SqlError::InvalidStringEncoding)?;

        return Ok(TableName(result));
    }
}

impl Deserialise for ColumnType {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        // A ColumnType is serialised as one byte
        if input.is_empty() {
            return Err(SqlError::InputTooShort(input.len(), 1));
        }

        let byte = *input.first().unwrap();

        *input = &input[1..];

        return match byte {
            1 => Ok(ColumnType::Int),
            2 => Ok(ColumnType::Decimal),
            3 => Ok(ColumnType::Text),
            4 => Ok(ColumnType::Bool),
            _ => Err(SqlError::NotATypeDiscriminator(byte)),
        };
    }
}

// Only reason we can't have a blanket implementation for Vec<T>
// is that ColumnValues requires the types to be known
// I can think of some middle way, but for now it's a TODO
impl Deserialise for Vec<ColumnType> {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        let count = usize::deserialise(input, None.into())?;

        let mut result = vec![];

        for _ in 0..count {
            result.push(
                ColumnType::deserialise(input, None.into())?
            );
        }

        return Ok(result);
    }
}


impl Deserialise for ColumnName {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        let length = usize::deserialise(input, None.into())?;

        if input.len() < length {
            return Err(SqlError::InputTooShort(input.len(), length));
        }

        let result = String::from_utf8(input[..length].to_vec())
            .map_err(SqlError::InvalidStringEncoding)?;

        *input = &input[length..];

        return Ok(ColumnName(result));
    }
}

impl Deserialise for Vec<ColumnName> {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        let count = usize::deserialise(input, None.into())?;

        let mut result = vec![];

        for _ in 0..count {
            result.push(
                ColumnName::deserialise(input, None.into())?
            );
        }

        return Ok(result);
    }
}


impl Deserialise for String {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        let length = usize::deserialise(input, None.into())?;

        if input.len() < length {
            return Err(SqlError::InputTooShort(input.len(), length));
        }

        let result = String::from_utf8(input[..length].to_vec())
            .map_err(SqlError::InvalidStringEncoding)?;

        *input = &input[length..];

        return Ok(result);
    }
}

impl Deserialise for bool {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self, SqlError> {
        if input.is_empty() {
            return Err(SqlError::InputTooShort(input.len(), 1));
        }

        let byte = *input.first().unwrap();

        let result = {
            match byte {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(SqlError::NotABoolean(byte))
            }
        }?;

        *input = &input[1..];

        return Ok(result);
    }
}

impl Deserialise for Row {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self, SqlError> {
        let result = Vec::<ColumnValue>::deserialise(input, options)?;

        return Ok(Row(result));
    }
}

impl Deserialise for Vec<Row> {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self, SqlError> {
        let count = usize::deserialise(input, None.into())?;

        let mut result = vec![];

        for _ in 0..count {
            result.push(
                Row::deserialise(input, options.clone())?
            );
        }

        return Ok(result);
    }
}

impl Deserialise for ColumnValue {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self, SqlError> {
        let column_type = match options {
            DO::ColumnType(column_type) => Ok(column_type),
            _ => Err(SqlError::InvalidParameter)
        }?;

        let result = match column_type {
            ColumnType::Int => ColumnValue::Int(usize::deserialise(input, None.into())?),
            ColumnType::Decimal => {
                let whole = usize::deserialise(input, None.into())?;
                let fractional = usize::deserialise(input, None.into())?;

                ColumnValue::Decimal(whole, fractional)
            },
            ColumnType::Text => ColumnValue::Str(String::deserialise(input, None.into())?),
            ColumnType::Bool => ColumnValue::Bool(bool::deserialise(input, None.into())?),
        };

        return Ok(result);
    }
}

impl Deserialise for Vec<ColumnValue> {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self, SqlError> {
        let types = match options {
            DO::ColumnTypes(types) => Ok(types),
            _ => Err(SqlError::InvalidParameter),
        }?;

        let count = usize::deserialise(input, None.into())?;

        if count != types.len() {
            return Err(SqlError::UnequalLengths(count, types.len()));
        }

        let mut result = vec![];

        for column_type in types {
            result.push(
                ColumnValue::deserialise(input, DO::ColumnType(column_type))?
            );
        }

        return Ok(result);
    }
}
