#[cfg(test)]
pub mod tests {
    use super::super::*;

    pub fn test_table() -> Table {
        return Table::new(
            Expression::Ident("test_table".into()),
            vec![
                Expression::ColumnDefinition("first".into(), ColumnType::Int),
                Expression::ColumnDefinition("second".into(), ColumnType::Bool),
            ],
        ).unwrap();
    }

    pub fn test_table_with_values() -> (Table, (Vec<ColumnValue>, Vec<ColumnValue>)) {
        let mut result = Table::new(
            Expression::Ident("test_table".into()),
            vec![
                Expression::ColumnDefinition("first".into(), ColumnType::Int),
                Expression::ColumnDefinition("second".into(), ColumnType::Bool),
            ],
        ).unwrap();

        let row1 = vec![
            ColumnValue::Int(5),
            ColumnValue::Bool(true),
        ];

        let row2 = vec![
            ColumnValue::Int(6),
            ColumnValue::Bool(false),
        ];

        result.insert_multiple(vec![row1.clone(), row2.clone()]).unwrap();

        return (result, (row1, row2));
    }

    pub fn test_row(values: Vec<ColumnValue>) -> Row {
        let (names, values)  = values.into_iter()
            .map(|row| {
                (ColumnName("test_name".into()), row)
            })
            .unzip();

        return Row::new(names, values).unwrap();
    }
}
