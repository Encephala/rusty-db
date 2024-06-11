#[cfg(test)]
pub mod tests {
    use sql_parse::parser::ColumnType;
    use super::super::database::{Table, Row, RowSet};
    use super::super::types::{TableName, ColumnName, DatabaseName, ColumnValue, ColumnDefinition};

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

    impl From<&str> for DatabaseName {
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

    pub fn test_row_set(values: Vec<Row>) -> RowSet {
        let names = std::iter::repeat("test_column_name".to_owned())
            .map(ColumnName)
            .take(values.len())
            .collect();

        return RowSet {
            names,
            values,
        };
    }
}
