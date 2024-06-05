use super::*;
use crate::ColumnType;

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
fn insert_and_drop_tables() {
    let mut env = RuntimeEnvironment::new();

    let table = Table::new(
        Expression::Ident("test_table".into()),
        vec![
            Expression::ColumnDefinition("a".into(), ColumnType::Int),
            Expression::ColumnDefinition("b".into(), ColumnType::Decimal),
        ],
    ).unwrap();

    env.insert(table.clone()).unwrap();

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
