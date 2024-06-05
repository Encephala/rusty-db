use super::*;
use super::super::expressions::{ColumnType, InfixOperator};
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
        ("SELECT bla from asdf;", S::Select {
            columns: E::Array(vec![E::Ident("bla".into())]),
            table: E::Ident("asdf".into()),
            where_clause: None,
        }),
        ("SELECT * from asdf;", S::Select {
            columns: E::AllColumns,
            table: E::Ident("asdf".into()),
            where_clause: None,
        }),
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
        ("CREATE DATABASE epic_db;", Some(S::Create {
            what: CreateType::Database,
            name: E::Ident("epic_db".into()),
            types: None,
        })),
        ("CREATE TABLE name (bool, int);", Some(S::Create{
            what: CreateType::Table,
            name: E::Ident("name".into()),
            types: Some(E::Array(vec![
                E::Type(ColumnType::Bool),
                E::Type(ColumnType::Int),
            ]))
        })),
        ("CREATE TABLE name;", None),
        ("CREATE TABLE blabla, blabla;", None),
        ("CREATE TABLE oops_no_semicolon", None),
        ("CREATE blabla;", None),
        ("CREATE TABLE 123", None),
    ];

    test_all_cases(Create, &inputs);
}

#[test]
fn insert_basic() {
    let inputs = [
        ("INSERT INTO bla VALUES (1, 'hey', 420.69);", Some(S::Insert {
            into: E::Ident("bla".into()),
            values: E::Array(vec![
                E::Int(1),
                E::Str("hey".into()),
                E::Decimal(420, 69),
            ])
        })),
        ("INSERT INTO bla VALUES (1, 'hey', 420.69);", Some(S::Insert {
            into: E::Ident("bla".into()),
            values: E::Array(vec![
                E::Int(1),
                E::Str("hey".into()),
                E::Decimal(420, 69),
            ])
        })),
        // Can't forget semicolon
        ("INSERT INTO bla VALUES ()", None),
        // Can't forget `INTO`
        ("INSERT bla VALUES ();", None),
    ];

    test_all_cases(Insert, &inputs);
}

#[test]
fn update_basic() {
    let inputs = [
        ("UPDATE tbl SET col = 1;", Some(S::Update {
            from: E::Ident("tbl".into()),
            columns: E::Array(vec![E::Ident("col".into())]),
            values: E::Array(vec![E::Int(1)]),
            where_clause: None,
        })),
        ("update tbl set col = 1 where other = 2;", Some(S::Update {
            from: E::Ident("tbl".into()),
            columns: E::Array(vec![E::Ident("col".into())]),
            values: E::Array(vec![E::Int(1)]),
            where_clause: Some(E::Where {
                left: E::Ident("other".into()).into(),
                operator: InfixOperator::Equals,
                right: E::Int(2).into(),
            }),
        })),
        ("UPDATE tbl set col1 = 1, col2 = 'value';", Some(S::Update {
            from: E::Ident("tbl".into()),
            columns: E::Array(vec![
                E::Ident("col1".into()),
                E::Ident("col2".into()),
            ]),
            values: E::Array(vec![
                E::Int(1),
                E::Str("value".into()),
            ]),
            where_clause: None,
        })),
        // Must end in semicolon
        ("UPDATE tbl set col1 = 1, col2 = 'value'", None),
    ];

    test_all_cases(Update, &inputs);
}

#[test]
fn delete_basic() {
    let inputs = [
        ("DELETE FROM tbl;", Some(S::Delete {
            from: E::Ident("tbl".into()),
            where_clause: None,
        })),
        ("DELETE FROM tbl WHERE col = 1;", Some(S::Delete {
            from: E::Ident("tbl".into()),
            where_clause: Some(E::Where {
                left: E::Ident("col".into()).into(),
                operator: InfixOperator::Equals,
                right: E::Int(1).into(),
            }),
        })),
        // Must end in semicolon
        ("DELETE FROM tbl", None),
    ];

    test_all_cases(Delete, &inputs);
}

#[test]
fn drop_basic() {
    let inputs = [
        ("DROP TABLE tbl;", Some(S::Drop {
            what: CreateType::Table,
            name: E::Ident("tbl".into()),
        })),
        ("DROP DATABASE db;", Some(S::Drop {
            what: CreateType::Database,
            name: E::Ident("db".into()),
        })),
        // Must end in semicolon
        ("DROP TABLE tbl", None),
    ];

    test_all_cases(Drop, &inputs);
}
