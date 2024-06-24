use super::*;
use crate::lexer::Lexer;

use Expression as E;

pub fn test_all_cases(parser: impl ExpressionParser, inputs: &[(&str, Option<Expression>)]) {
    inputs.iter().for_each(|test_case| {
        let result = parser.parse(&mut Lexer::lex(test_case.0).as_slice());

        assert_eq!(result, test_case.1);
    });
}

#[test]
fn number_parser_basic() {
    let inputs = [
        ("1", Some(E::Int(1))),
        ("69420", Some(E::Int(69420))),
        ("asdf", None),
        ("5.321", Some(E::Decimal(5, 321))),
        ("5.3.2.1", None),
    ];

    test_all_cases(Number, &inputs);
}

#[test]
fn string_parser_basic() {
    let inputs = [
        ("'asdf'", Some(E::Str("asdf".into()))),
        ("'hey hi_hello'", Some(E::Str("hey hi_hello".into()))),
        ("c", None),
        ("'c'", Some(E::Str("c".into()))),
        ("1", None),
    ];

    test_all_cases(Str, &inputs);
}

#[test]
fn bool_parser_basic() {
    let inputs = [
        ("true", Some(E::Bool(true))),
        ("tru", None),
        ("false", Some(E::Bool(false))),
        ("truefalse", None),
    ];

    test_all_cases(Bool, &inputs);
}

#[test]
fn type_parser_basic() {
    let inputs = [
        ("INT", Some(E::Type(ColumnType::Int))),
        ("INTeger", Some(E::Type(ColumnType::Int))),
        ("bool", Some(E::Type(ColumnType::Bool))),
        ("decimal", Some(E::Type(ColumnType::Decimal))),
        ("text", Some(E::Type(ColumnType::Text))),
        ("asdf", None),
    ];

    test_all_cases(Type, &inputs);
}

#[test]
fn type_parser_list() {
    let input = "bool, int, integer, text";

    let result = Type.multiple().parse(&mut Lexer::lex(input).as_slice());

    assert_eq!(
        result,
        Some(E::Array(vec![
            E::Type(ColumnType::Bool),
            E::Type(ColumnType::Int),
            E::Type(ColumnType::Int),
            E::Type(ColumnType::Text),
        ]))
    )
}

#[test]
fn identifier_parser_basic() {
    let inputs = [
        ("blablabla", Some(E::Ident("blablabla".into()))),
        ("Bl_a", Some(E::Ident("Bl_a".into()))),
        ("1abc", None),
    ];

    test_all_cases(Identifier, &inputs);
}

#[test]
fn column_definition_parser_basic() {
    let inputs = [
        (
            "asdf INT",
            Some(E::ColumnDefinition("asdf".into(), ColumnType::Int)),
        ),
        (
            "jkl TEXT",
            Some(E::ColumnDefinition("jkl".into(), ColumnType::Text)),
        ),
    ];

    test_all_cases(ColumnDefinition, &inputs);
}

#[test]
fn parse_all_columns_character() {
    let inputs = [("*", Some(E::AllColumns)), ("asdf", None)];

    test_all_cases(AllColumn, &inputs);
}

#[test]
fn column_parser_basic() {
    let inputs = [
        ("*", Some(E::Array(vec![E::AllColumns]))),
        (
            "column_name",
            Some(E::Array(vec![E::Ident("column_name".into())])),
        ),
        (
            "otherColumnName",
            Some(E::Array(vec![E::Ident("otherColumnName".into())])),
        ),
        (
            "a, b",
            Some(E::Array(vec![E::Ident("a".into()), E::Ident("b".into())])),
        ),
        // No trailing commas
        ("a, b,", None),
    ];

    test_all_cases(Column, &inputs);
}

#[test]
fn where_parser_basic() {
    let inputs = [
        (
            "WHERE a = 5",
            Some(E::Where {
                left: E::Ident("a".into()).into(),
                operator: InfixOperator::Equals,
                right: E::Int(5).into(),
            }),
        ),
        (
            "WHERE column >= other_column",
            Some(E::Where {
                left: E::Ident("column".into()).into(),
                operator: InfixOperator::GreaterThanEqual,
                right: E::Ident("other_column".into()).into(),
            }),
        ),
        (
            "WHERE 10 <> 5",
            Some(E::Where {
                left: E::Int(10).into(),
                operator: InfixOperator::NotEqual,
                right: E::Int(5).into(),
            }),
        ),
        ("WHERE column", None),
        ("column <> other_column", None),
        ("WHERE * = 0", None),
    ];

    test_all_cases(Where, &inputs);
}

#[test]
fn array_basic() {
    let inputs = [
        (
            "(1, 2.3, 'hey', 4)",
            Some(E::Array(vec![
                E::Int(1),
                E::Decimal(2, 3),
                E::Str("hey".into()),
                E::Int(4),
            ])),
        ),
        // Allow trailing commas
        ("(1,)", Some(E::Array(vec![E::Int(1)]))),
    ];

    test_all_cases(Array, &inputs);
}
