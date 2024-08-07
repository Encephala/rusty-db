use super::super::expressions::{ColumnType, InfixOperator};
use super::*;
use crate::lexer::Lexer;

use {Expression as E, Statement as S};

pub fn test_all_cases(parser: impl StatementParser, inputs: &[(&str, Option<Statement>)]) {
    inputs.iter().for_each(|test_case| {
        let result = parser.parse(Lexer::lex(test_case.0).as_slice());

        assert_eq!(result, test_case.1);
    });
}

#[test]
fn select_basic() {
    let inputs = [
        (
            "SELECT bla from asdf;",
            S::Select {
                columns: E::Array(vec![E::Ident("bla".into())]),
                table: E::Ident("asdf".into()),
                where_clause: None,
            },
        ),
        (
            "SELECT * from asdf;",
            S::Select {
                columns: E::AllColumns,
                table: E::Ident("asdf".into()),
                where_clause: None,
            },
        ),
    ];

    inputs.into_iter().for_each(|test_case| {
        let result = Select.parse(&Lexer::lex(test_case.0));

        assert_eq!(result, Some(test_case.1));
    });
}

#[test]
fn select_with_where() {
    let input = "SELECT bla FROM asdf WHERE a > b;";

    let result = Select.parse(Lexer::lex(input).as_slice());

    assert_eq!(
        result,
        Some(S::Select {
            columns: E::Array(vec![E::Ident("bla".into())]),
            table: E::Ident("asdf".into()),
            where_clause: Some(E::Where {
                left: E::Ident("a".into()).into(),
                operator: InfixOperator::GreaterThan,
                right: E::Ident("b".into()).into(),
            })
        })
    )
}

#[test]
fn create_basic() {
    let inputs = [
        (
            "CREATE DATABASE epic_db;",
            Some(S::Create {
                what: CreateType::Database,
                name: E::Ident("epic_db".into()),
                columns: None,
            }),
        ),
        (
            "CREATE TABLE name (a bool, b int);",
            Some(S::Create {
                what: CreateType::Table,
                name: E::Ident("name".into()),
                columns: Some(E::Array(vec![
                    E::ColumnDefinition("a".into(), ColumnType::Bool),
                    E::ColumnDefinition("b".into(), ColumnType::Int),
                ])),
            }),
        ),
        ("CREATE TABLE name;", None),
        ("CREATE TABLE blabla, blabla;", None),
        ("CREATE TABLE oops_no_semicolon(id INT)", None),
        ("CREATE blabla;", None),
        ("CREATE TABLE 123", None),
    ];

    test_all_cases(Create, &inputs);
}

#[test]
fn create_table_foreign_key() {
    let inputs = [(
        "CREATE TABLE tbl (
                id INT,
                foreign_id INT,
                FOREIGN KEY (foreign_id) REFERENCES other_tbl(id)
            );",
        Some(S::Create {
            what: CreateType::Table,
            name: E::Ident("tbl".into()),
            columns: Some(E::Array(vec![
                E::ColumnDefinition("id".into(), ColumnType::Int),
                E::ColumnDefinition("foreign_id".into(), ColumnType::Int),
                E::ForeignKeyConstraint {
                    column: Box::new(E::Ident("foreign_id".into())),
                    foreign_table: Box::new(E::Ident("other_tbl".into())),
                    foreign_column: Box::new(E::Ident("id".into())),
                },
            ])),
        }),
    )];

    test_all_cases(Create, &inputs);
}

#[test]
fn insert_basic() {
    let inputs = [
        (
            "INSERT INTO bla (a, b, c) VALUES (1, 'hey', 420.69);",
            Some(S::Insert {
                into: E::Ident("bla".into()),
                columns: Some(E::Array(vec![
                    E::Ident("a".into()),
                    E::Ident("b".into()),
                    E::Ident("c".into()),
                ])),
                values: E::Array(vec![E::Array(vec![
                    E::Int(1),
                    E::Str("hey".into()),
                    E::Decimal(420, 69),
                ])]),
            }),
        ),
        // Can't forget semicolon
        ("INSERT INTO bla VALUES ()", None),
        // Can't forget `INTO`
        ("INSERT bla VALUES ();", None),
    ];

    test_all_cases(Insert, &inputs);
}

#[test]
fn insert_all_columns() {
    let inputs = [(
        "INSERT INTO tbl VALUES (1, 420.69);",
        Some(S::Insert {
            into: E::Ident("tbl".into()),
            columns: None,
            values: E::Array(vec![E::Array(vec![E::Int(1), E::Decimal(420, 69)])]),
        }),
    )];

    test_all_cases(Insert, &inputs);
}

#[test]
fn insert_multiple_simultaneously() {
    let inputs = [(
        "INSERT INTO bla(a, b)VALUES (true, 420.69), (false, 69.420);",
        Some(S::Insert {
            into: E::Ident("bla".into()),
            columns: Some(E::Array(vec![E::Ident("a".into()), E::Ident("b".into())])),
            values: E::Array(vec![
                E::Array(vec![E::Bool(true), E::Decimal(420, 69)]),
                E::Array(vec![E::Bool(false), E::Decimal(69, 420)]),
            ]),
        }),
    )];

    test_all_cases(Insert, &inputs);
}

#[test]
fn update_basic() {
    let inputs = [
        (
            "UPDATE tbl SET col = 1;",
            Some(S::Update {
                from: E::Ident("tbl".into()),
                columns: E::Array(vec![E::Ident("col".into())]),
                values: E::Array(vec![E::Int(1)]),
                where_clause: None,
            }),
        ),
        (
            "update tbl set col = 1 where other = 2;",
            Some(S::Update {
                from: E::Ident("tbl".into()),
                columns: E::Array(vec![E::Ident("col".into())]),
                values: E::Array(vec![E::Int(1)]),
                where_clause: Some(E::Where {
                    left: E::Ident("other".into()).into(),
                    operator: InfixOperator::Equals,
                    right: E::Int(2).into(),
                }),
            }),
        ),
        (
            "UPDATE tbl set col1 = 1, col2 = 'value';",
            Some(S::Update {
                from: E::Ident("tbl".into()),
                columns: E::Array(vec![E::Ident("col1".into()), E::Ident("col2".into())]),
                values: E::Array(vec![E::Int(1), E::Str("value".into())]),
                where_clause: None,
            }),
        ),
        // Must end in semicolon
        ("UPDATE tbl set col1 = 1, col2 = 'value'", None),
    ];

    test_all_cases(Update, &inputs);
}

#[test]
fn delete_basic() {
    let inputs = [
        (
            "DELETE FROM tbl;",
            Some(S::Delete {
                from: E::Ident("tbl".into()),
                where_clause: None,
            }),
        ),
        (
            "DELETE FROM tbl WHERE col = 1;",
            Some(S::Delete {
                from: E::Ident("tbl".into()),
                where_clause: Some(E::Where {
                    left: E::Ident("col".into()).into(),
                    operator: InfixOperator::Equals,
                    right: E::Int(1).into(),
                }),
            }),
        ),
        // Must end in semicolon
        ("DELETE FROM tbl", None),
    ];

    test_all_cases(Delete, &inputs);
}

#[test]
fn drop_basic() {
    let inputs = [
        (
            "DROP TABLE tbl;",
            Some(S::Drop {
                what: CreateType::Table,
                name: E::Ident("tbl".into()),
            }),
        ),
        (
            "DROP DATABASE db;",
            Some(S::Drop {
                what: CreateType::Database,
                name: E::Ident("db".into()),
            }),
        ),
        // Must end in semicolon
        ("DROP TABLE tbl", None),
    ];

    test_all_cases(Drop, &inputs);
}
