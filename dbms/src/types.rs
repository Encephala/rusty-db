//! Helpers to convert from Expressions to various types that are more convenient to use.
//!
//! The helper `impl_owned` makes it so conversion might require cloning, even if converting owned expression,
//! but fuck it it's easier to write this way and converting statements isn't going to be the bottleneck, executing is
//! (hopefully).
use std::any::type_name;

use sql_parse::parser::{ColumnType, InfixOperator};

use super::{Expression, SqlError};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnName(pub String);
impl TryFrom<&Expression> for ColumnName {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::Ident(name) => Ok(ColumnName(name.clone())),
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<ColumnName>(),
            )),
        };
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct TableName(pub String);

impl TryFrom<&Expression> for TableName {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::Ident(name) => Ok(TableName(name.clone())),
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<TableName>(),
            )),
        };
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
// TODO: Validating the name
// At least that it doesn't contain slashes
// I'm sure rust has some function to validate that a string is a valid filename
pub struct DatabaseName(pub String);

impl TryFrom<&Expression> for DatabaseName {
    type Error = SqlError;

    fn try_from(value: &Expression) -> Result<Self> {
        return match value {
            Expression::Ident(name) => Ok(DatabaseName(name.clone())),
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<DatabaseName>(),
            )),
        };
    }
}

// impl_owned!(DatabaseName);

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
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
                let columns: Vec<ColumnName> = columns
                    .iter()
                    .map(|column| column.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Ok(ColumnSelector::Name(columns))
            }
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<ColumnSelector>(),
            )),
        };
    }
}

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
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<ColumnValue>(),
            )),
        };
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
            }
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<ColumnDefinition>(),
            )),
        };
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub struct ForeignKeyConstraint(ColumnName, TableName, ColumnName);

impl TryFrom<&Expression> for ForeignKeyConstraint {
    type Error = SqlError;

    fn try_from(value: &Expression) -> std::result::Result<Self, Self::Error> {
        return match value {
            Expression::ForeignKeyConstraint { column, foreign_table, foreign_column } => {
                let column_name = ColumnName::try_from(column.as_ref())?;
                let foreign_table = TableName::try_from(foreign_table.as_ref())?;
                let foreign_column = ColumnName::try_from(foreign_column.as_ref())?;

                Ok(ForeignKeyConstraint(column_name, foreign_table, foreign_column))
            },
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<ForeignKeyConstraint>()
            )),
        }
    }
}

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
            Expression::Where {
                left,
                operator,
                right,
            } => {
                // TODO: might be a literal. I have to rework this whole Where thingy
                let left: ColumnName = left.as_ref().try_into()?;

                let right: ColumnValue = right.as_ref().try_into()?;

                return Ok(Self {
                    left,
                    operator: *operator,
                    right,
                });
            }
            _ => Err(SqlError::ImpossibleConversion(
                value.clone(),
                type_name::<Where>(),
            )),
        };
    }
}

#[derive(Debug)]
pub struct PreparedWhere {
    pub left: usize,
    pub operator: InfixOperator,
    pub right: ColumnValue,
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub struct TableSchema {
    pub name: TableName,
    pub column_names: Vec<ColumnName>,
    pub types: Vec<ColumnType>,
}

#[cfg(test)]
mod tests {
    use sql_parse::parser::Expression;

    use crate::SqlError;

    use super::*;

    #[test]
    fn names_from_invalid_expression() {
        let input = Expression::Int(5);

        let name = ColumnName::try_from(&input);

        if let Err(SqlError::ImpossibleConversion(expression, "dbms::types::ColumnName")) = name {
            if let Expression::Int(5) = expression {
            } else {
                panic!("Wrong expression");
            }
        } else {
            panic!("Incorrect return type");
        }

        let name = TableName::try_from(&input);

        if let Err(SqlError::ImpossibleConversion(expression, "dbms::types::TableName")) = name {
            if let Expression::Int(5) = expression {
            } else {
                panic!("Wrong expression");
            }
        } else {
            panic!("Incorrect return type");
        }

        let name = DatabaseName::try_from(&input);

        if let Err(SqlError::ImpossibleConversion(expression, "dbms::types::DatabaseName")) = name {
            if let Expression::Int(5) = expression {
            } else {
                panic!("Wrong expression");
            }
        } else {
            panic!("Incorrect return type");
        }
    }

    #[test]
    fn column_selector_from_array_expression() {
        let input = Expression::Array(vec![
            Expression::Ident("a".into()),
            Expression::Ident("deez nuts".into()),
        ]);

        let selector = ColumnSelector::try_from(&input).unwrap();

        assert_eq!(
            selector,
            ColumnSelector::Name(vec!["a".into(), "deez nuts".into(),])
        );
    }

    #[test]
    fn column_selector_from_invalid_expression() {
        let input = Expression::Int(5);

        let selector = ColumnSelector::try_from(&input);

        if let Err(SqlError::ImpossibleConversion(expression, "dbms::types::ColumnSelector")) =
            selector
        {
            if let Expression::Int(5) = expression {
            } else {
                panic!("Wrong expression");
            }
        } else {
            panic!("Incorrect return type");
        }
    }
}
