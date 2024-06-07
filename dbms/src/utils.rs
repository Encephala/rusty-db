#[cfg(test)]
pub mod tests {
    use sql_parse::ColumnType;
    use super::super::table::{Table, Row};
    use super::super::types::{TableName, ColumnName, ColumnValue, ColumnDefinition};

    impl From<&str> for TableName {
        fn from(value: &str) -> Self {
            return Self(value.into());
        }
    }

    impl From<&str> for ColumnName {
        fn from(value: &str) -> Self {
            return Self(value.into());
        }
    }

    impl From<usize> for ColumnValue {
        fn from(value: usize) -> Self {
            return Self::Int(value);
        }
    }

    impl From<(usize, usize)> for ColumnValue {
        fn from(value: (usize, usize)) -> Self {
            return Self::Decimal(value.0, value.1);
        }
    }

    impl From<&str> for ColumnValue {
        fn from(value: &str) -> Self {
            return Self::Str(value.into());
        }
    }

    impl From<bool> for ColumnValue {
        fn from(value: bool) -> Self {
            return Self::Bool(value);
        }
    }

    pub fn test_table() -> Table {
        return Table::new(
            "test_table".into(),
            vec![
                ColumnDefinition("first".into(), ColumnType::Int),
                ColumnDefinition("second".into(), ColumnType::Bool),
            ],
        ).unwrap();
    }

    pub fn test_table_with_values() -> (Table, (Vec<ColumnValue>, Vec<ColumnValue>)) {
        let mut result = Table::new(
            "test_table".into(),
            vec![
                ColumnDefinition("first".into(), ColumnType::Int),
                ColumnDefinition("second".into(), ColumnType::Bool),
            ],
        ).unwrap();

        let row1 = vec![
            5.into(),
            true.into()
        ];

        let row2 = vec![
            6.into(),
            false.into(),
        ];

        result.insert_multiple(
            &None,
            vec![row1.clone(), row2.clone()]
        ).unwrap();

        return (result, (row1, row2));
    }

    pub fn test_row(values: Vec<ColumnValue>) -> Row {
        let (names, values)  = values.into_iter()
            .map(|row| {
                ("test_name".into(), row)
            })
            .unzip();

        return Row::new(names, values).unwrap();
    }
}
