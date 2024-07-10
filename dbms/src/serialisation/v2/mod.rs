//! Essentially the same as v1, except it uses u64 instead of usize for sizes
#[cfg(test)]
mod tests;

use sql_parse::parser::ColumnType;

use crate::{
    database::{Row, RowSet, Table},
    types::{ColumnName, ColumnValue, TableName, TableSchema},
    Result, SqlError,
};

use super::Serialise;

#[derive(Debug)]
pub struct V2;

impl Serialise for V2 {
    fn serialise_table(&self, value: &Table) -> Vec<u8> {
        return value.serialise();
    }

    fn serialise_rowset(&self, value: &RowSet) -> Vec<u8> {
        return value.serialise();
    }

    fn deserialise_table(&self, input: &mut &[u8]) -> Result<Table> {
        return Table::deserialise(input, None.into());
    }

    fn deserialise_rowset(&self, input: &mut &[u8]) -> Result<RowSet> {
        return RowSet::deserialise(input, None.into());
    }
}

#[derive(Debug, Clone)]
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

trait V2Serialise {
    fn serialise(&self) -> Vec<u8>;
}

trait V2Deserialise {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self>
    where
        Self: Sized;
}

impl V2Serialise for Table {
    fn serialise(&self) -> Vec<u8> {
        let mut result = vec![];

        let schema = self.schema.serialise();

        result.extend(schema);

        let values = self.values.serialise();

        result.extend(values);

        return result;
    }
}

impl V2Serialise for TableSchema {
    fn serialise(&self) -> Vec<u8> {
        let mut result = vec![];

        let name = self.name.serialise();

        result.extend(name);

        let types = self.types.serialise();

        result.extend(types);

        let names = self.column_names.serialise();

        result.extend(names);

        return result;
    }
}

impl V2Serialise for TableName {
    fn serialise(&self) -> Vec<u8> {
        return self.0.serialise();
    }
}

impl V2Serialise for ColumnType {
    fn serialise(&self) -> Vec<u8> {
        // Start counting at 1 to make sure uninitialised data isn't a valid type
        // (for what it's worth)
        return match self {
            ColumnType::Int => vec![1],
            ColumnType::Decimal => vec![2],
            ColumnType::Text => vec![3],
            ColumnType::Bool => vec![4],
        };
    }
}

impl V2Serialise for ColumnName {
    fn serialise(&self) -> Vec<u8> {
        return self.0.serialise();
    }
}

impl V2Serialise for Row {
    fn serialise(&self) -> Vec<u8> {
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
//
// Ah well fixing that means writing a specialised Vec<ColumnValue> parser as well,
// see above about negative impl. Doing it now would just be unnecessary complexity

impl V2Serialise for ColumnValue {
    fn serialise(&self) -> Vec<u8> {
        return match self {
            ColumnValue::Int(value) => (*value as u64).serialise(),
            ColumnValue::Decimal(whole, fractional) => {
                let mut result = (*whole as u64).serialise();

                result.extend((*fractional as u64).serialise());

                result
            }
            ColumnValue::Str(value) => {
                let mut result = (value.len() as u64).serialise();

                result.extend(value.as_bytes());

                result
            }
            ColumnValue::Bool(value) => vec![*value as u8],
        };
    }
}

impl V2Serialise for u64 {
    fn serialise(&self) -> Vec<u8> {
        // https://stackoverflow.com/questions/72631065/how-to-convert-a-u32-array-to-a-u8-array-in-place
        let mut result = Vec::with_capacity(8);

        for byte in self.to_le_bytes() {
            result.push(byte)
        }

        return result;
    }
}

impl V2Serialise for String {
    fn serialise(&self) -> Vec<u8> {
        let mut result = (self.len() as u64).serialise();

        result.extend(self.bytes());

        return result;
    }
}

impl<T: V2Serialise> V2Serialise for Vec<T> {
    fn serialise(&self) -> Vec<u8> {
        let mut result = vec![];

        // First store total count
        result.extend((self.len() as u64).serialise());

        for t in self {
            let bytes = t.serialise();

            result.extend(bytes);
        }

        return result;
    }
}

impl V2Serialise for RowSet {
    fn serialise(&self) -> Vec<u8> {
        let mut result = self.types.serialise();

        result.extend(self.names.serialise());

        result.extend(self.values.serialise());

        return result;
    }
}

impl V2Deserialise for Table {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        let schema = TableSchema::deserialise(input, DO::None)?;

        let values = Vec::<Row>::deserialise(input, DO::ColumnTypes(schema.types.clone()))?;

        return Ok(Table { schema, values });
    }
}

impl V2Deserialise for TableSchema {
    fn deserialise(input: &mut &[u8], _: DeserialisationOptions) -> Result<Self>
    where
        Self: Sized,
    {
        let name = TableName::deserialise(input, None.into())?;

        let types = Vec::<ColumnType>::deserialise(input, None.into())?;

        let column_names = Vec::<ColumnName>::deserialise(input, None.into())?;

        return Ok(TableSchema {
            name,
            column_names,
            types,
            constraints: vec![], // TODO
        });
    }
}

impl V2Deserialise for u64 {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        if input.len() < 8 {
            return Err(SqlError::InputTooShort(input.len(), 8));
        }

        // try_into to convert slice into fixed-length array
        let bytes: [u8; 8] = input[..8]
            .try_into()
            .map_err(SqlError::SliceConversionError)?;

        let result = u64::from_le_bytes(bytes);

        *input = &input[8..];

        return Ok(result);
    }
}

impl V2Deserialise for TableName {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        let result = String::deserialise(input, None.into())?;

        return Ok(TableName(result));
    }
}

impl V2Deserialise for ColumnType {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
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
impl V2Deserialise for Vec<ColumnType> {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        let count = u64::deserialise(input, None.into())?;

        let mut result = vec![];

        for _ in 0..count {
            result.push(ColumnType::deserialise(input, None.into())?);
        }

        return Ok(result);
    }
}

impl V2Deserialise for ColumnName {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        let result = String::deserialise(input, None.into())?;

        return Ok(ColumnName(result));
    }
}

impl V2Deserialise for Vec<ColumnName> {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        let count = u64::deserialise(input, None.into())?;

        let mut result = vec![];

        for _ in 0..count {
            result.push(ColumnName::deserialise(input, None.into())?);
        }

        return Ok(result);
    }
}

impl V2Deserialise for String {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        let length = u64::deserialise(input, None.into())?;

        if input.len() < length as usize {
            return Err(SqlError::InputTooShort(input.len(), length as usize));
        }

        // https://doc.rust-lang.org/book/ch08-02-strings.html
        // strings are UTF8 in rust
        let result = String::from_utf8(input[..length as usize].to_vec())
            .map_err(SqlError::NotAValidString)?;

        *input = &input[length as usize..];

        return Ok(result);
    }
}

impl V2Deserialise for bool {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self> {
        if input.is_empty() {
            return Err(SqlError::InputTooShort(input.len(), 1));
        }

        let byte = *input.first().unwrap();

        let result = {
            match byte {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(SqlError::NotABoolean(byte)),
            }
        }?;

        *input = &input[1..];

        return Ok(result);
    }
}

impl V2Deserialise for Row {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self> {
        let result = Vec::<ColumnValue>::deserialise(input, options)?;

        return Ok(Row(result));
    }
}

impl V2Deserialise for Vec<Row> {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self> {
        let count = u64::deserialise(input, None.into())?;

        let mut result = vec![];

        for _ in 0..count {
            result.push(Row::deserialise(input, options.clone())?);
        }

        return Ok(result);
    }
}

impl V2Deserialise for ColumnValue {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self> {
        let column_type = match options {
            DO::ColumnType(column_type) => Ok(column_type),
            _ => Err(SqlError::InvalidParameter),
        }?;

        let result = match column_type {
            ColumnType::Int => ColumnValue::Int(u64::deserialise(input, None.into())? as usize),
            ColumnType::Decimal => {
                let whole = u64::deserialise(input, None.into())?;
                let fractional = u64::deserialise(input, None.into())?;

                ColumnValue::Decimal(whole as usize, fractional as usize)
            }
            ColumnType::Text => ColumnValue::Str(String::deserialise(input, None.into())?),
            ColumnType::Bool => ColumnValue::Bool(bool::deserialise(input, None.into())?),
        };

        return Ok(result);
    }
}

impl V2Deserialise for Vec<ColumnValue> {
    fn deserialise(input: &mut &[u8], options: DO) -> Result<Self> {
        let types = match options {
            DO::ColumnTypes(types) => Ok(types),
            _ => Err(SqlError::InvalidParameter),
        }?;

        let count = u64::deserialise(input, None.into())?;

        if count != types.len() as u64 {
            return Err(SqlError::UnequalLengths(count as usize, types.len()));
        }

        let mut result = vec![];

        for column_type in types {
            result.push(ColumnValue::deserialise(
                input,
                DO::ColumnType(column_type),
            )?);
        }

        return Ok(result);
    }
}

impl V2Deserialise for RowSet {
    fn deserialise(input: &mut &[u8], _: DO) -> Result<Self>
    where
        Self: Sized,
    {
        let types = Vec::<ColumnType>::deserialise(input, None.into()).unwrap();

        let names = Vec::<ColumnName>::deserialise(input, None.into())?;

        let values = Vec::<Row>::deserialise(input, DO::ColumnTypes(types.clone()))?;

        return Ok(Self {
            types,
            names,
            values,
        });
    }
}
