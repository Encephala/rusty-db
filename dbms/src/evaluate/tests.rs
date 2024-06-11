use super::*;
use super::super::database::{Database, Row};
use super::super::types::ColumnDefinition;
use sql_parse::parser::{InfixOperator, ColumnType};
use crate::utils::tests::*;

#[test]
fn create_and_drop_tables_basic() {
    let mut db = Database::new("db".into());

    let table = Table::new(
        "test_table".into(),
        vec![
            ColumnDefinition("a".into(), ColumnType::Int),
            ColumnDefinition("b".into(), ColumnType::Decimal),
        ],
    ).unwrap();

    db.create(table.clone()).unwrap();

    assert_eq!(
        db.tables.len(),
        1
    );

    assert_eq!(
        db.tables.get(&table.name.0),
        Some(&table)
    );

    db.drop_table(table.name.clone()).unwrap();

    assert_eq!(
        db.tables.len(),
        0
    );

    assert_eq!(
        db.tables.get(&table.name.0),
        None
    );
}

#[test]
fn insert_into_table_basic() {
    let mut db = Database::new("db".into());

    db.create(test_table()).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![],
    );

    db.insert("test_table".into(), None, vec![vec![
        ColumnValue::Int(69),
        ColumnValue::Bool(false),
    ]]).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![
            Row(vec![ColumnValue::Int(69), ColumnValue::Bool(false)]),
        ]
    );
}

#[test]
fn select_from_table_basic() {
    let mut db = Database::new("db".into());

    let (table, (row1, row2)) = test_table_with_values();

    db.create(table).unwrap();

    assert_eq!(
        db.select("test_table".into(), ColumnSelector::AllColumns, None).unwrap(),
        test_row_set(vec![Row(row1.clone()), Row(row2.clone())])
    );

    assert_eq!(
        db.select(
            "test_table".into(),
            ColumnSelector::Name(vec![ColumnName("first".into())]),
            None
        ).unwrap(),
        test_row_set(vec![
            Row(vec![ColumnValue::Int(5)]),
            Row(vec![ColumnValue::Int(6)])
        ])
    );

    assert_eq!(
        db.select(
            "test_table".into(),
            ColumnSelector::AllColumns,
            Some(Where {
                left: "second".into(),
                operator: InfixOperator::Equals,
                right: true.into(),
            })
        ).unwrap(),
        test_row_set(vec![
            Row(vec![ColumnValue::Int(5), ColumnValue::Bool(true)]),
        ])
    );

    assert_eq!(
        db.select(
            "test_table".into(),
            ColumnSelector::Name(vec![ColumnName("first".into())]),
            Some(Where {
                left: "second".into(),
                operator: InfixOperator::Equals,
                right: true.into(),
            })
        ).unwrap(),
        test_row_set(vec![
            Row(vec![ColumnValue::Int(5)]),
        ])
    );
}

#[test]
fn delete_from_table_basic() {
    let mut db = Database::new("db".into());

    let (table, (row1, row2)) = test_table_with_values();
    db.create(table).unwrap();

    db.delete("test_table".into(), None).unwrap();

    assert_eq!(
        db.tables.len(),
        1
    );

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![]
    );

    db.insert("test_table".into(), None, vec![row1.clone(), row2.clone()]).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![Row(row1.clone()), Row(row2.clone())]
    );

    db.delete("test_table".into(), Some(Where {
        left: "second".into(),
        operator: InfixOperator::Equals,
        right: false.into(),
    })).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![Row(row1)]
    );
}

#[test]
fn update_table_basic() {
    let mut db = Database::new("db".into());

    let (table, (row1, row2)) = test_table_with_values();

    db.create(table.clone()).unwrap();

    db.update(
        "test_table".into(),
        vec![ColumnName("first".into())],
        vec![ColumnValue::Int(69)],
        None
    ).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![
            Row(vec![
                ColumnValue::Int(69),
                ColumnValue::Bool(true)
            ]),
            Row(vec![
                ColumnValue::Int(69),
                ColumnValue::Bool(false)
            ]),
        ]
    );

    db.update(
        "test_table".into(),
        vec![ColumnName("first".into()), ColumnName("second".into())],
        vec![ColumnValue::Int(420), ColumnValue::Bool(true)],
        None
    ).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![
            Row(vec![
                ColumnValue::Int(420),
                ColumnValue::Bool(true)
            ]),
            Row(vec![
                ColumnValue::Int(420),
                ColumnValue::Bool(true)
            ]),
        ]
    );

    // Reset table
    db.drop_table("test_table".into()).unwrap();
    db.create(table).unwrap();

    db.update(
        "test_table".into(),
        vec![],
        vec![],
        None,
    ).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![Row(row1), Row(row2)]
    );

    db.update(
        "test_table".into(),
        vec![ColumnName("first".into())],
        vec![ColumnValue::Int(0)],
        Some(Where {
            left: "second".into(),
            operator: InfixOperator::Equals,
            right: false.into(),
        })
    ).unwrap();

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![
            Row(vec![
                ColumnValue::Int(5),
                ColumnValue::Bool(true),
            ]),
            Row(vec![
                ColumnValue::Int(0),
                ColumnValue::Bool(false),
            ]),
        ]
    );
}
