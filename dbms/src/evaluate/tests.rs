use std::path::PathBuf;

use super::*;
use super::super::database::{Database, Row};
use super::super::types::ColumnDefinition;
use sql_parse::parser::{InfixOperator, ColumnType};
use crate::utils::tests::*;
use crate::{evaluate::{Execute, ExecutionResult}, persistence::{FileSystem, PersistenceManager}, serialisation::{SerialisationManager, Serialiser}};

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
fn create_table_duplicate_name() {
    let mut db = Database::new("test_db".into());

    let table = Table::new(
        "test_table1".into(),
        vec![]
    ).unwrap();

    db.create(table.clone()).unwrap();

    if let Err(SqlError::DuplicateTable(name)) = db.create(table) {
        assert_eq!(
            name,
            "test_table1"
        );
    } else {
        panic!("Wrong result returned");
    }
}

fn new_persistence_manager() -> Box<dyn PersistenceManager> {
    return Box::new(FileSystem::new(
        SerialisationManager(Serialiser::V2),
        PathBuf::from("/tmp/rusty-db-tests")
    ));
}

fn test_db() -> Database {
    let mut db = Database::new("test_db".into());

    let table = test_table_with_values().0;

    db.create(table).unwrap();

    return db;
}

#[test]
fn create_basic() {
    let mut db = Database::new("test_db".into());

    let table = test_table();

    db.create(table).unwrap();
}

#[tokio::test]
async fn create_db_statement() {
    let statement = Statement::Create {
        what: CreateType::Database,
        name: Expression::Ident("test_db".into()),
        columns: None,
    };

    let persistence = new_persistence_manager();
    let result = statement.execute(None, persistence.as_ref()).await.unwrap();

    assert_eq!(
        result,
        ExecutionResult::CreateDatabase("test_db".into()),
    );
}

#[tokio::test]
async fn create_table_statement() {
    let mut db = test_db();

    let statement = Statement::Create {
        what: CreateType::Table,
        name: Expression::Ident("other_test_table".into()),
        columns: Some(Expression::Array(vec![
            Expression::ColumnDefinition("first".into(), ColumnType::Int),
            Expression::ColumnDefinition("second".into(), ColumnType::Bool),
        ])),
    };

    let persistence = new_persistence_manager();

    let result = statement.execute(Some(&mut db), persistence.as_ref()).await.unwrap();

    assert_eq!(
        result,
        ExecutionResult::None,
    );

    test_create_table_statement_no_db(&statement, persistence.as_ref()).await;
}

async fn test_create_table_statement_no_db(statement: &Statement, persistence: &dyn PersistenceManager) {
    let result = statement.execute(None, persistence).await;

    dbg!(&result);
    assert!(matches!(
        result,
        Err(SqlError::NoDatabaseSelected),
    ));
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

#[tokio::test]
async fn insert_statement() {
    let mut db = test_db();

    let statement = Statement::Insert {
        into: Expression::Ident("test_table".into()),
        columns: None,
        values: Expression::Array(vec![
            Expression::Array(vec![
                Expression::Int(7),
                Expression::Bool(true),
            ]),
            Expression::Array(vec![
                Expression::Int(7),
                Expression::Bool(false),
            ]),
        ]),
    };

    let persistence = new_persistence_manager();
    let result = statement.execute(
        Some(&mut db),
        persistence.as_ref(),
    ).await.unwrap();

    assert_eq!(
        result,
        ExecutionResult::None,
    );

    test_insert_statement_no_db(&statement, persistence.as_ref()).await;
}

async fn test_insert_statement_no_db(statement: &Statement, manager: &dyn PersistenceManager) {
    let failed_result = statement.execute(
        None,
        manager
    ).await;

    dbg!(&failed_result);
    assert!(matches!(
        failed_result,
        Err(SqlError::NoDatabaseSelected)
    ));
}

#[test]
fn select_from_table_basic() {
    let mut db = Database::new("db".into());

    let (table, (row1, row2)) = test_table_with_values();

    db.create(table).unwrap();

    assert_eq!(
        db.select("test_table".into(), ColumnSelector::AllColumns, None).unwrap(),
        test_row_set(vec![Row(row1.clone()), Row(row2.clone())]).unwrap()
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
        ]).unwrap()
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
        ]).unwrap()
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
        ]).unwrap()
    );
}

#[tokio::test]
async fn select_statement() {
    let mut db = test_db();

    let statement = Statement::Select {
        table: Expression::Ident("test_table".into()),
        columns: Expression::AllColumns,
        where_clause: Some(Expression::Where {
            left: Box::new(Expression::Ident("second".into())),
            operator: InfixOperator::Equals,
            right: Box::new(Expression::Bool(true))
        })
    };

    let persistence = new_persistence_manager();
    let result = statement.execute(Some(&mut db), persistence.as_ref()).await.unwrap();

    assert_eq!(
        result,
        ExecutionResult::Select(RowSet {
            types: vec![
                ColumnType::Int,
                ColumnType::Bool,
            ],
            names: vec![
                "first".into(),
                "second".into(),
            ],
            values: vec![
                Row(vec![
                    ColumnValue::Int(5),
                    ColumnValue::Bool(true),
                ])
            ],
        })
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

#[tokio::test]
async fn delete_statement() {
    let mut db = test_db();

    let statement = Statement::Delete {
        from: Expression::Ident("test_table".into()),
        where_clause: Some(Expression::Where {
            left: Box::new(Expression::Ident("first".into())),
            operator: InfixOperator::Equals,
            right: Box::new(Expression::Int(5)),
        })
    };

    let persistence = new_persistence_manager();
    let result = statement.execute(Some(&mut db), persistence.as_ref()).await.unwrap();

    assert_eq!(
        result,
        ExecutionResult::None,
    );

    assert_eq!(
        db.tables.len(),
        1,
    );

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![
            Row(vec![
                ColumnValue::Int(6),
                ColumnValue::Bool(false),
            ]),
        ],
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

#[tokio::test]
async fn update_statement() {
    let mut db = test_db();

    let statement = Statement::Update {
        from: Expression::Ident("test_table".into()),
        columns: Expression::Array(vec![
            Expression::Ident("first".into()),
            Expression::Ident("second".into()),
        ]),
        values: Expression::Array(vec![
            Expression::Int(69),
            Expression::Bool(true),
        ]),
        where_clause: Some(Expression::Where {
            left: Box::new(Expression::Ident("second".into())),
            operator: InfixOperator::Equals,
            right: Box::new(Expression::Bool(false)),
        })
    };

    let persistence = new_persistence_manager();
    let result = statement.execute(Some(&mut db), persistence.as_ref()).await.unwrap();

    assert_eq!(
        result,
        ExecutionResult::None,
    );

    assert_eq!(
        db.tables.get("test_table").unwrap().values,
        vec![
            Row(vec![ColumnValue::Int(5), ColumnValue::Bool(true)]),
            Row(vec![ColumnValue::Int(69), ColumnValue::Bool(true)]),
        ]
    );
}
