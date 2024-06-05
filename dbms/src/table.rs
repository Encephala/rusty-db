use super::{SqlError, Expression, ColumnType, InfixOperator};
use super::conversions::{ColumnName, ColumnValue, Where};

#[derive(Debug, Clone)]
pub struct Row {
    pub names: Vec<String>,
    pub values: Vec<ColumnValue>,
}

// Ignore name of columns when comparing rows
impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        return self.values == other.values;
    }
}

impl Row {
    fn new(names: Vec<String>, values: Vec<ColumnValue>) -> Result<Self, SqlError> {
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

    fn select_by_names(&self, columns: Vec<ColumnName>) -> Result<Row, SqlError> {
        let names = columns.into_iter().map(|column_name| {
            let ColumnName(name) = column_name;
            name
        }).collect::<Vec<_>>();

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
    types: Vec<ColumnType>,
    names: Vec<String>,
    values: Vec<Vec<ColumnValue>>,
}

impl Table {
    pub fn new(names: Vec<Expression>, types: Vec<ColumnType>) -> Result<Self, SqlError> {
        if names.len() != types.len() {
            return Err(SqlError::UnequalLengths(names.len(), types.len()));
        }

        let names = names.into_iter().map(|identifier| {
            if let Expression::Ident(name) = identifier {
                name
            } else {
                panic!("Creating table with column name {identifier:?} that isn't an identifier")
            }
        }).collect();

        return Ok(Table {
            types,
            names,
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

        self.values.push(row);

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
    pub fn select(&self, columns: Vec<Expression>, condition: Option<Expression>) -> Result<Vec<Row>, SqlError> {
        let column_indices: Vec<_>;
        if let Some(Expression::AllColumns) = columns.get(0) {
            column_indices = (0..self.types.len()).collect()
        } else {
            column_indices = columns.iter().flat_map(|name| {
                if let Expression::Ident(name) = name {
                    self.names.iter().position(|self_name| self_name == name)
                } else {
                    panic!("Selecting from table with non-ident {name:?}");
                }
            })
            .collect()
        };


        let mut result = vec![];

        for row in &self.values {
            let row = Row::new(
                self.names.clone(),
                row.clone()
            )?;

            if row.matches(&condition)? {
                result.push(row.select(&column_indices)?);
            }
        }

        return Ok(result);
    }

    pub fn update(&mut self, new_values: Row, condition: Option<Expression>) -> Result<(), SqlError> {
        // self.values = self.values.into_iter().map(|row| {
        //     todo!();
        // });

        todo!();
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::InfixOperator;

    fn test_table() -> Table {
        return Table::new(
            vec![
                Expression::Ident("first".into()),
                Expression::Ident("second".into()),
            ],
            vec![
                ColumnType::Int,
                ColumnType::Bool,
            ]
        ).unwrap();
    }

    #[test]
    fn insert_basic() {
        let mut table = test_table();

        let row1 = vec![
            ColumnValue::Int(5),
            ColumnValue::Bool(true),
        ];

        let row2 = vec![
            ColumnValue::Int(6),
            ColumnValue::Bool(false),
        ];

        if let Err(message) = table.insert_multiple(vec![row1.clone(), row2.clone()]) {
            panic!("Failed to insert rows ({message:?})");
        }

        assert_eq!(
            table.values,
            vec![row1, row2]
        );
    }

    #[test]
    fn insert_check_types() {
        let mut table = test_table();

        let row1 = vec![
            ColumnValue::Bool(true),
            ColumnValue::Int(5),
        ];

        let row2 = vec![
            ColumnValue::Int(6),
            ColumnValue::Str("false".into()),
        ];

        let result1 = table.insert(row1);
        let result2 = table.insert(row2);

        assert!(matches!(result1, Err(SqlError::IncompatibleTypes(_, _))));
        assert!(matches!(result2, Err(SqlError::IncompatibleTypes(_, _))));

        assert_eq!(
            table.values,
            Vec::<Vec<_>>::new()
        );
    }

    #[test]
    fn select_basic() {
        let mut table = test_table();

        let row1 = vec![
            ColumnValue::Int(5),
            ColumnValue::Bool(true),
        ];

        let row2 = vec![
            ColumnValue::Int(6),
            ColumnValue::Bool(false),
        ];

        table.insert(row1.clone()).unwrap();
        table.insert(row2.clone()).unwrap();

        let all = table.select(vec![Expression::AllColumns], None).unwrap();

        // Janky comparison to not have to move row1 and row2
        assert_eq!(
            all,
            vec![Row::new(table.names.clone(), row1.clone()).unwrap(), Row::new(table.names.clone(), row2).unwrap()]
        );

        let where_bool_true = table.select(
            vec![Expression::AllColumns],
            Some(Expression::Where {
                left: Expression::Ident("second".into()).into(),
                operator: InfixOperator::Equals,
                right: Expression::Bool(true).into(),
            })
        ).unwrap();

        assert_eq!(
            where_bool_true,
            vec![Row::new(table.names.clone(), row1).unwrap()]
        );

        let only_int_five = table.select(
            vec![Expression::Ident("first".into())],
            Some(Expression::Where {
                left: Expression::Ident("first".into()).into(),
                operator: InfixOperator::Equals,
                right: Expression::Int(5).into(),
            })
        ).unwrap();

        assert_eq!(
            only_int_five,
            vec![Row::new(
                vec!["first".into()],
                vec![ColumnValue::Int(5)]
            ).unwrap()]
        );

        let none = table.select(
            vec![],
            None,
        ).unwrap();

        assert_eq!(
            none,
            vec![
                Row::new(vec![], vec![]).unwrap(),
                Row::new(vec![], vec![]).unwrap(),
            ]
        )
    }
}
