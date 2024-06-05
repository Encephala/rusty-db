//! Helpers to convert from Expressions to various types that are more convenient to use.
use std::any::type_name;

use sql_parse::{ColumnType, InfixOperator};

use super::{Expression, SqlError};

macro_rules! impl_owned {
    ($t:ty) => {
        impl TryFrom<Expression> for $t {
            type Error = SqlError;

            fn try_from(value: Expression) -> Result<Self, Self::Error> {
                return (&value).try_into();
            }
        }
    };
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnName(pub String);
impl TryFrom<&Expression> for ColumnName {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self, Self::Error> {
        return match value {
            Expression::Ident(name) => Ok(ColumnName(name.clone())),
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<ColumnName>())),
        };
    }
}

impl_owned!(ColumnName);

#[derive(Debug, Clone, PartialEq)]
pub struct TableName(pub String);


#[derive(Debug, PartialEq)]
pub enum ColumnSelector {
    AllColumns,
    Name(Vec<ColumnName>),
}


#[derive(Debug, PartialEq, Clone)]
pub enum ColumnValue {
    Int(usize),
    Decimal(usize, usize),
    Str(String),
    Bool(bool),
}
impl TryFrom<&Expression> for ColumnValue {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self, Self::Error> {
        use Expression as E;
        return match value {
            E::Int(value) => Ok(ColumnValue::Int(*value)),
            E::Decimal(whole, fractional) => Ok(ColumnValue::Decimal(*whole, *fractional)),
            E::Str(value) => Ok(ColumnValue::Str(value.clone())),
            E::Bool(value) => Ok(ColumnValue::Bool(*value)),
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<ColumnValue>())),
        };
    }
}

impl_owned!(ColumnValue);


impl From<ColumnValue> for ColumnType {
    fn from(value: ColumnValue) -> Self {
        return (&value).into();
    }
}

impl From<&ColumnValue> for ColumnType {
    fn from(value: &ColumnValue) -> Self {
        return match value {
            ColumnValue::Int(_) => ColumnType::Int,
            ColumnValue::Decimal(_, _) => ColumnType::Decimal,
            ColumnValue::Str(_) => ColumnType::Text,
            ColumnValue::Bool(_) => ColumnType::Bool,
        };
    }
}


#[derive(Debug)]
// TODO: this is janky and hacky to only support column = value comparisons
pub struct Where {
    pub left: ColumnName,
    pub operator: InfixOperator,
    pub right: ColumnValue,
}
impl TryFrom<&Expression> for Where {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self, Self::Error> {
        return match value {
            Expression::Where { left, operator, right } => {
                // TODO: might be a literal. I have to rework this whole Where thingy
                let left: ColumnName = left.as_ref().try_into()?;

                let right: ColumnValue = right.as_ref().try_into()?;

                return Ok(Self {
                    left,
                    operator: *operator,
                    right,
                })
            },
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<Where>()))
        }
    }
}

impl_owned!(Where);
