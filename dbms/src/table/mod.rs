#[cfg(test)]
mod tests;

use super::{SqlError, Expression, ColumnType, InfixOperator};
use super::types::{ColumnName, TableName, ColumnSelector, ColumnValue, Where};

#[derive(Debug, Clone)]
pub struct Row {
    names: Vec<ColumnName>,
    values: Vec<ColumnValue>,
}

// Ignore name of columns when comparing rows
impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        return self.values == other.values;
    }
}

impl Row {
    pub fn new(names: Vec<ColumnName>, values: Vec<ColumnValue>) -> Result<Self, SqlError> {
        if names.len() != values.len() {
            return Err(SqlError::UnequalLengths(names.len(), values.len()));
        }

        return Ok(Row {
            names,
            values,
        });
    }

    fn select(&self, columns: &[usize]) -> Result<Row, SqlError> {
        for index in columns {
            if *index >= self.values.len() {
                return Err(SqlError::IndexOutOfBounds(*index, self.values.len()));
            }
        };

        let (names, values): (Vec<_>, Vec<_>) = self.names.iter()
            .zip(&self.values)
            .enumerate()
            .filter(|(index, _)| {
                columns.contains(index)
            })
            .map(|(_, pair)| (pair.0.clone(), pair.1.clone()))
            .unzip();

        return Row::new(names, values);
    }

    fn select_by_names(&self, names: Vec<ColumnName>) -> Result<Row, SqlError> {
        for name in names.iter() {
            if !self.names.contains(name) {
                return Err(SqlError::NameDoesNotExist(name.clone(), self.names.clone()));
            }
        }

        let values = self.names.iter()
            .zip(&self.values)
            .filter(|(name, _)| {
                names.contains(name)
            })
            .map(|(_, value)| value.clone())
            .collect();

        return Row::new(names, values);
    }

    fn update(&mut self,
        columns: &[usize],
        new_values: Vec<ColumnValue>,
        condition: &Option<Expression>
    ) -> Result<(), SqlError> {
        assert_eq!(columns.len(), new_values.len());

        if !self.matches(condition)? {
            return Ok(())
        }

        let self_length = self.values.len();

        for (index, new_value) in columns.iter().zip(new_values) {
            *self.values.get_mut(*index).ok_or(SqlError::IndexOutOfBounds(*index, self_length))? = new_value;
        }

        return Ok(());
    }

    fn matches(&self, condition: &Option<Expression>) -> Result<bool, SqlError> {
        if let Some(expression) = condition {
            let Where { left, operator, right } = expression.try_into()?;

            // TODO: All the other comparisons
            // But should probably come after properly implementing:
            // - parsing expressions recursively
            // - then evaluating expressions properly
            return match operator {
                InfixOperator::Equals => self.evaluate_equals(left, right),
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

    fn evaluate_equals(&self, left: ColumnName, right: ColumnValue) -> Result<bool, SqlError> {
        let Row { values, ..  } = self.select_by_names(vec![left])?;

        // Values will always have length 1
        let value = values.get(0).unwrap();

        return Ok(value == &right);
    }
}


#[derive(Debug)]
pub struct Table {
    pub name: TableName,
    pub types: Vec<ColumnType>,
    pub column_names: Vec<ColumnName>,
    pub values: Vec<Row>,
}

impl Table {
    pub fn new(name: Expression, columns: Vec<Expression>) -> Result<Self, SqlError> {
        let name = if let Expression::Ident(name) = name {
            TableName(name)
        } else {
            panic!("Creating table with name {name:?} that isn't an identifier")
        };

        let (column_names, types):  (Vec<_>, Vec<_>) = columns.into_iter().map(|identifier| {
            if let Expression::ColumnDefinition(name, column_type) = identifier {
                (ColumnName(name), column_type)
            } else {
                panic!("Creating table with column definition {identifier:?} that isn't a definition")
            }
        }).unzip();

        let mut unique_names = std::collections::HashSet::new();
        for name in column_names.iter() {
            if !unique_names.insert(name) {
                return Err(SqlError::ColumnNameNotUnique(name.clone()));
            }
        }

        return Ok(Table {
            name,
            types,
            column_names,
            values: vec![],
        });
    }

    pub fn insert(&mut self, row: Vec<ColumnValue>) -> Result<(), SqlError> {
        let types = row.iter()
            .map(|row| row.into())
            .collect::<Vec<_>>();

        if types != self.types {
            return Err(SqlError::IncompatibleTypes(types, self.types.clone()));
        }

        self.values.push(Row::new(self.column_names.clone(), row)?);

        return Ok(());
    }

    pub fn insert_multiple(&mut self, rows: Vec<Vec<ColumnValue>>) -> Result<(), SqlError> {
        for row in rows {
            self.insert(row)?;
        }

        return Ok(());
    }

    // I don't like that columns is necessarily a vec, it should be a vec of idents or an Expression::AllColumns
    // Also this whole method kinda sucks donkey dick, wtf am I looking at
    pub fn select(&self, columns: ColumnSelector, condition: Option<Expression>) -> Result<Vec<Row>, SqlError> {
        let column_indices: Vec<_> = match columns {
            ColumnSelector::AllColumns => (0..self.types.len()).collect(),
            ColumnSelector::Name(names) => {
                names.iter().flat_map(|name| {
                    self.column_names.iter().position(|self_name| self_name == name)
                }).collect()
            }
        };

        let mut result = vec![];

        for row in &self.values {
            if row.matches(&condition)? {
                result.push(row.select(&column_indices)?);
            }
        }

        return Ok(result);
    }

    pub fn update(&mut self,
        columns: Vec<ColumnName>,
        new_values: Vec<ColumnValue>,
        condition: Option<Expression>
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


        for row in &mut self.values {
            row.update(&column_indices, new_values.clone(), &condition)?;
        }

        return Ok(());
    }

    pub fn delete(&mut self, condition: Option<Expression>) -> Result<(), SqlError> {
        let mut remove_indices = vec![];

        for (index, row) in self.values.iter().enumerate() {
            if row.matches(&condition)? {
                remove_indices.push(index);
            }
        }

        for index in remove_indices.into_iter().rev() {
            self.values.remove(index);
        }

        return Ok(());
    }
}
