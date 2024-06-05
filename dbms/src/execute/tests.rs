use super::*;
use crate::ColumnType;
use crate::utils::tests::{test_row, test_table};

// Pretty nice for testing
impl Clone for Table {
    fn clone(&self) -> Self {
        Self { name: self.name.clone(), types: self.types.clone(), column_names: self.column_names.clone(), values: self.values.clone() }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.types == other.types && self.column_names == other.column_names && self.values == other.values
    }
}

#[test]
fn create_and_drop_tables_basic() {
    let mut env = RuntimeEnvironment::new();

    let table = Table::new(
        Expression::Ident("test_table".into()),
        vec![
            Expression::ColumnDefinition("a".into(), ColumnType::Int),
            Expression::ColumnDefinition("b".into(), ColumnType::Decimal),
        ],
    ).unwrap();

    env.create(table.clone()).unwrap();

    assert_eq!(
        env.0.len(),
        1
    );

    assert_eq!(
        env.0.get(&table.name.0),
        Some(&table)
    );

    env.drop(&table.name.0).unwrap();

    assert_eq!(
        env.0.len(),
        0
    );

    assert_eq!(
        env.0.get(&table.name.0),
        None
    );
}

#[test]
fn insert_into_table_basic() {
    let mut env = RuntimeEnvironment::new();

    env.create(test_table()).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![],
    );

    env.insert("test_table", vec![
        ColumnValue::Int(69),
        ColumnValue::Bool(false),
    ]).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![
            test_row(vec![ColumnValue::Int(69), ColumnValue::Bool(false)]),
        ]
    );
}
