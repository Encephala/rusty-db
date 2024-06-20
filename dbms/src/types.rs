//! Helpers to convert from Expressions to various types that are more convenient to use.
//!
//! The helper `impl_owned` makes it so conversion might require cloning, even if converting owned expression,
//! but fuck it it's easier to write this way and converting statements isn't going to be the bottleneck, executing is
//! (hopefully).
use std::any::type_name;

use sql_parse::parser::{ColumnType, InfixOperator};

use crate::Result;
use super::{Expression, SqlError};

// Implement owned conversion with macro rather than borrowed conversion,
// because an owned value can be borrowed but a borrowed value can't be owned.
// Well I guess you could clone it but ah well, would rather clone a String than an Expression I guess
macro_rules! impl_owned {
    ($t:ty) => {
        impl TryFrom<Expression> for $t {
            type Error = SqlError;

            fn try_from(value: Expression) -> Result<Self> {
                return (&value).try_into();
            }
        }
    };
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnName(pub String);
impl TryFrom<&Expression> for ColumnName {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::Ident(name) => Ok(ColumnName(name.clone())),
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<ColumnName>())),
        };
    }
}

impl_owned!(ColumnName);

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct TableName(pub String);

impl TryFrom<&Expression> for TableName {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::Ident(name) => Ok(TableName(name.clone())),
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<TableName>())),
        };
    }
}

impl_owned!(TableName);


#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DatabaseName(pub String);

impl TryFrom<&Expression> for DatabaseName {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::Ident(name) => Ok(DatabaseName(name.clone())),
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<DatabaseName>())),
        };
    }
}

impl_owned!(DatabaseName);


#[derive(Debug)]
pub enum ColumnSelector {
    AllColumns,
    Name(Vec<ColumnName>),
}

impl TryFrom<&Expression> for ColumnSelector {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::AllColumns => Ok(ColumnSelector::AllColumns),
            Expression::Array(columns) => {
                let columns: Vec<ColumnName> = columns.iter()
                    .map(|column| column.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Ok(ColumnSelector::Name(columns))
            }
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<ColumnSelector>())),
        };
    }
}

impl_owned!(ColumnSelector);


#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum ColumnValue {
    Int(usize),
    Decimal(usize, usize),
    Str(String),
    Bool(bool),
}
impl TryFrom<&Expression> for ColumnValue {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
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
pub struct ColumnDefinition(pub ColumnName, pub ColumnType);

impl TryFrom<&Expression> for ColumnDefinition {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::ColumnDefinition(name, column_type) => {
                Ok(Self(ColumnName(name.clone()), *column_type))
            },
            _ => Err(SqlError::ImpossibleConversion(value.clone(), type_name::<ColumnDefinition>()))
        };
    }
}

impl_owned!(ColumnDefinition);


#[derive(Debug)]
// TODO: this is janky and hacky to only support <column> <op> <value> comparisons
pub struct Where {
    pub left: ColumnName,
    pub operator: InfixOperator,
    pub right: ColumnValue,
}

impl TryFrom<&Expression> for Where {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
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

#[derive(Debug)]
pub struct PreparedWhere {
    pub left: usize,
    pub operator: InfixOperator,
    pub right: ColumnValue,
}
