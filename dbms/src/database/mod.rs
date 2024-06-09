#[cfg(test)]
mod tests;

use std::collections::HashMap;

use sql_parse::{ColumnType, InfixOperator};

use super::SqlError;
use super::types::{ColumnName, TableName, DatabaseName, ColumnSelector, ColumnValue, ColumnDefinition, Where, PreparedWhere};

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub struct Row(pub Vec<ColumnValue>);

impl Row {
    fn select(&self, columns: &[usize]) -> Result<Row, SqlError>{
        for index in columns {
            if *index >= self.0.len() {
                return Err(SqlError::IndexOutOfBounds(*index, self.0.len()));
            }
        };

        let values = self.0.iter()
            .enumerate()
            .filter_map(|(index, values)| {
                if columns.contains(&index) {
                    Some(values)
                } else {
                    None
                }
            })
            .cloned()
            .collect();

        return Ok(Row(values));
    }

    fn update(&mut self,
        columns: &[usize],
        new_values: Vec<ColumnValue>,
        condition: &Option<PreparedWhere>
    ) -> Result<(), SqlError> {
        assert_eq!(columns.len(), new_values.len());

        if !self.matches(condition)? {
            return Ok(())
        }

        let self_length = self.0.len();

        for (index, new_value) in columns.iter().zip(new_values) {
            *self.0.get_mut(*index).ok_or(SqlError::IndexOutOfBounds(*index, self_length))? = new_value;
        }

        return Ok(());
    }

    fn matches(&self, condition: &Option<PreparedWhere>) -> Result<bool, SqlError> {
        if let Some(where_clause) = condition {
            let PreparedWhere { left, operator, right } = where_clause;

            // TODO: All the other comparisons
            // But should probably come after properly implementing:
            return match operator {
                InfixOperator::Equals => self.evaluate_equals(*left, right),
                InfixOperator::NotEqual => todo!(),
                InfixOperator::LessThan => todo!(),
                InfixOperator::LessThanEqual => todo!(),
                InfixOperator::GreaterThan => todo!(),
                InfixOperator::GreaterThanEqual => todo!(),
            }
        } else {
            return Ok(true);
        }
    }

    fn evaluate_equals(&self, left: usize, right: &ColumnValue) -> Result<bool, SqlError> {
        let result = self.select(&[left])?;

        // Result will always have length one
        let value = result.0.get(0).unwrap();

        return Ok(value == right);
    }
}

#[derive(Debug)]
pub struct RowSet {
    pub names: Vec<ColumnName>,
    pub values: Vec<Row>
}
#[cfg(test)]
// Ignore column names when comparing RowSets
impl PartialEq for RowSet {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub struct Table {
    pub name: TableName,
    pub column_names: Vec<ColumnName>,
    pub types: Vec<ColumnType>,
    pub values: Vec<Row>,
}

impl Table {
    pub fn new(name: TableName, columns: Vec<ColumnDefinition>) -> Result<Self, SqlError> {
        let (column_names, types):  (Vec<_>, Vec<_>) = columns.into_iter()
            .map(|column_definition| (column_definition.0, column_definition.1))
            .unzip();

        let mut unique_names = std::collections::HashSet::new();
        for name in column_names.iter() {
            if !unique_names.insert(name) {
                return Err(SqlError::ColumnNameNotUnique(name.clone()));
            }
        }

        return Ok(Table {
            name,
            column_names,
            types,
            values: vec![],
        });
    }

    pub fn insert(&mut self, columns: &Option<Vec<ColumnName>>, row: Vec<ColumnValue>) -> Result<(), SqlError> {
        let types = row.iter()
            .map(|row| row.into())
            .collect::<Vec<_>>();

        if types != self.types {
            return Err(SqlError::IncompatibleTypes(types, self.types.clone()));
        }

        // TODO: Check that all non-nullable columns were passed
        // But that implies first implementing nullable
        if let Some(columns) = columns {
            if columns.len() != row.len() {
                return Err(SqlError::UnequalLengths(columns.len(), row.len()));
            }
        }

        self.values.push(Row(row));

        return Ok(());
    }

    pub fn insert_multiple(
        &mut self,
        columns: &Option<Vec<ColumnName>>,
        values: Vec<Vec<ColumnValue>>
    ) -> Result<(), SqlError> {
        for row in values {
            self.insert(columns, row)?;
        }

        return Ok(());
    }

    fn prepare_where_clause(&self, clause: Where) -> Result<PreparedWhere, SqlError> {
        let Where { left, operator, right } = clause;

        let left_index = self.column_names.iter()
            .position(|self_name| self_name == &left)
            .ok_or(SqlError::NameDoesNotExist(left, self.column_names.clone()))?;

        return Ok(PreparedWhere { left: left_index, operator, right });
    }

    // I don't like that columns is necessarily a vec, it should be a vec of identifiers or an Expression::AllColumns
    // Also this whole method kinda sucks donkey dick, wtf am I looking at
    pub fn select(&self, columns: ColumnSelector, condition: Option<Where>) -> Result<RowSet, SqlError> {
        let column_indices: Vec<_> = match columns {
            ColumnSelector::AllColumns => (0..self.types.len()).collect(),
            ColumnSelector::Name(names) => {
                names.iter().flat_map(|name| {
                    self.column_names.iter().position(|self_name| self_name == name)
                }).collect()
            }
        };

        let prepared_condition = if let Some(condition) = condition {
            Some(self.prepare_where_clause(condition)?)
        } else {
            None
        };

        let mut rows = vec![];

        for row in &self.values {
            if row.matches(&prepared_condition)? {
                rows.push(row.select(&column_indices)?);
            }
        }

        return Ok(RowSet {
            names: self.column_names.clone(),
            values: rows,
        });
    }

    pub fn update(&mut self,
        columns: Vec<ColumnName>,
        new_values: Vec<ColumnValue>,
        condition: Option<Where>
    ) -> Result<(), SqlError> {
        let new_types: Vec<ColumnType> = new_values.iter().map(|value| value.into()).collect();

        let column_indices = columns.iter().flat_map(|name| {
            self.column_names.iter().position(|self_name| self_name == name)
        }).collect::<Vec<_>>();


        let self_types: Vec<_> = self.types.iter()
            .enumerate()
            .filter_map(|(index, value)| {
                if column_indices.contains(&index) {
                    Some(*value)
                } else {
                    None
                }
            }).collect();

        if self_types != new_types {
            return Err(SqlError::IncompatibleTypes(new_types, self.types.clone()));
        }

        let prepared_condition = if let Some(condition) = condition {
            Some(self.prepare_where_clause(condition)?)
        } else {
            None
        };

        for row in &mut self.values {
            row.update(&column_indices, new_values.clone(), &prepared_condition)?;
        }

        return Ok(());
    }

    pub fn delete(&mut self, condition: Option<Where>) -> Result<(), SqlError> {
        let mut remove_indices = vec![];

        let prepared_condition = if let Some(condition) = condition {
            Some(self.prepare_where_clause(condition)?)
        } else {
            None
        };

        for (index, row) in self.values.iter().enumerate() {
            if row.matches(&prepared_condition)? {
                remove_indices.push(index);
            }
        }

        for index in remove_indices.into_iter().rev() {
            self.values.remove(index);
        }

        return Ok(());
    }
}

#[derive(Debug)]
pub struct Database {
    pub name: DatabaseName,
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new(name: DatabaseName) -> Self {
        return Self {
            name,
            tables: HashMap::new(),
        };
    }
}
