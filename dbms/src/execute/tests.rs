use super::*;
use crate::{ColumnType, InfixOperator};
use crate::utils::tests::{test_row, test_table, test_table_with_values};

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

    env.insert("test_table", vec![vec![
        ColumnValue::Int(69),
        ColumnValue::Bool(false),
    ]]).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![
            test_row(vec![ColumnValue::Int(69), ColumnValue::Bool(false)]),
        ]
    );
}

#[test]
fn select_from_table_basic() {
    let mut env = RuntimeEnvironment::new();

    let (table, (row1, row2)) = test_table_with_values();

    env.create(table).unwrap();

    assert_eq!(
        env.select("test_table", ColumnSelector::AllColumns, None).unwrap(),
        vec![test_row(row1.clone()), test_row(row2.clone())]
    );

    assert_eq!(
        env.select(
            "test_table",
            ColumnSelector::Name(vec![ColumnName("first".into())]),
            None
        ).unwrap(),
        vec![
            test_row(vec![ColumnValue::Int(5)]),
            test_row(vec![ColumnValue::Int(6)])
        ]
    );

    assert_eq!(
        env.select(
            "test_table",
            ColumnSelector::AllColumns,
            Some(Expression::Where {
                left: Expression::Ident("second".into()).into(),
                operator: InfixOperator::Equals,
                right: Expression::Bool(true).into(),
            })
        ).unwrap(),
        vec![
            test_row(vec![ColumnValue::Int(5), ColumnValue::Bool(true)]),
        ]
    );

    assert_eq!(
        env.select(
            "test_table",
            ColumnSelector::Name(vec![ColumnName("first".into())]),
            Some(Expression::Where {
                left: Expression::Ident("second".into()).into(),
                operator: InfixOperator::Equals,
                right: Expression::Bool(true).into(),
            })
        ).unwrap(),
        vec![
            test_row(vec![ColumnValue::Int(5)]),
        ]
    );
}

#[test]
fn delete_from_table_basic() {
    let mut env = RuntimeEnvironment::new();

    let (table, (row1, row2)) = test_table_with_values();
    env.create(table).unwrap();

    env.delete("test_table", None).unwrap();

    assert_eq!(
        env.0.len(),
        1
    );

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![]
    );

    env.insert("test_table", vec![row1.clone(), row2.clone()]).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![test_row(row1.clone()), test_row(row2.clone())]
    );

    env.delete("test_table", Some(Expression::Where {
        left: Expression::Ident("second".into()).into(),
        operator: InfixOperator::Equals,
        right: Expression::Bool(false).into(),
    })).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![test_row(row1)]
    );
}

#[test]
fn update_table_basic() {
    let mut env = RuntimeEnvironment::new();

    let (table, (row1, row2)) = test_table_with_values();

    env.create(table.clone()).unwrap();

    env.update(
        "test_table",
        vec![ColumnName("first".into())],
        vec![ColumnValue::Int(69)],
        None
    ).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![
            test_row(vec![
                ColumnValue::Int(69),
                ColumnValue::Bool(true)
            ]),
            test_row(vec![
                ColumnValue::Int(69),
                ColumnValue::Bool(false)
            ]),
        ]
    );

    env.update(
        "test_table",
        vec![ColumnName("first".into()), ColumnName("second".into())],
        vec![ColumnValue::Int(420), ColumnValue::Bool(true)],
        None
    ).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![
            test_row(vec![
                ColumnValue::Int(420),
                ColumnValue::Bool(true)
            ]),
            test_row(vec![
                ColumnValue::Int(420),
                ColumnValue::Bool(true)
            ]),
        ]
    );

    // Reset table
    env.drop("test_table").unwrap();
    env.create(table).unwrap();

    env.update(
        "test_table",
        vec![],
        vec![],
        None,
    ).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![test_row(row1), test_row(row2)]
    );

    env.update(
        "test_table",
        vec![ColumnName("first".into())],
        vec![ColumnValue::Int(0)],
        Some(Expression::Where {
            left: Expression::Ident("second".into()).into(),
            operator: InfixOperator::Equals,
            right: Expression::Bool(false).into(),
        })
    ).unwrap();

    assert_eq!(
        env.0.get("test_table").unwrap().values,
        vec![
            test_row(vec![
                ColumnValue::Int(5),
                ColumnValue::Bool(true),
            ]),
            test_row(vec![
                ColumnValue::Int(0),
                ColumnValue::Bool(false),
            ]),
        ]
    );
}