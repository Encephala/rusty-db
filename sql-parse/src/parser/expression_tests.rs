
use super::expressions::*;
use super::combinators::Chain;
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
        ("1", Some(E::IntLiteral(1))),
        ("69420", Some(E::IntLiteral(69420))),
        ("asdf", None),
        ("5.321", Some(E::DecimalLiteral(5, 321))),
        ("5.3.2.1", None),
    ];

    test_all_cases(NumberLiteral, &inputs);
}

#[test]
fn string_parser_basic() {
    let inputs = [
        ("'asdf'", Some(E::StrLiteral("asdf".into()))),
        ("'hey hi_hello'", Some(E::StrLiteral("hey hi_hello".into()))),
        ("c", None),
        ("'c'", Some(E::StrLiteral("c".into()))),
        ("1", None),
    ];

    test_all_cases(StrLiteral, &inputs);
}

#[test]
fn type_parser_basic() {
    let inputs = [
        ("INT", Some(E::Type(ColumnType::Int))),
        ("INTeger", Some(E::Type(ColumnType::Int))),
        ("bool", Some(E::Type(ColumnType::Bool))),
        ("decimal", Some(E::Type(ColumnType::Decimal))),
        ("Varchar(10)", Some(E::Type(ColumnType::VarChar(10)))),
        ("Varchar", None),
        ("asdf", None),
    ];

    test_all_cases(Type, &inputs);
}

#[test]
fn type_parser_list() {
    let input = "bool, int, integer, varchar(10)";

    let result = Type.multiple().parse(
        &mut Lexer::lex(input).as_slice()
    );

    assert_eq!(
        result,
        Some(E::Array(vec![
            E::Type(ColumnType::Bool),
            E::Type(ColumnType::Int),
            E::Type(ColumnType::Int),
            E::Type(ColumnType::VarChar(10)),
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
fn parse_all_columns_character() {
    let inputs = [
        ("*", Some(E::AllColumns)),
        ("asdf", None)
    ];

    test_all_cases(AllColumn, &inputs);
}

#[test]
fn column_parser_basic() {
    let inputs = [
        ("*", Some(E::Array(vec![E::AllColumns]))),
        ("column_name", Some(E::Array(vec![E::Ident("column_name".into())]))),
        ("otherColumnName", Some(E::Array(vec![E::Ident("otherColumnName".into())]))),
        ("a, b", Some(E::Array(vec![
            E::Ident("a".into()),
            E::Ident("b".into()),
        ]))),
        // No trailing commas
        ("a, b,", None),
    ];

    test_all_cases(Column, &inputs);
}

#[test]
fn where_parser_basic() {
    let inputs = [
        ("WHERE a = 5", Some(E::Where {
            left: E::Ident("a".into()).into(),
            operator: InfixOperator::Equals,
            right: E::IntLiteral(5).into()
        })),
        ("WHERE column >= other_column", Some(E::Where {
            left: E::Ident("column".into()).into(),
            operator: InfixOperator::GreaterThanEqual,
            right: E::Ident("other_column".into()).into()
        })),
        ("WHERE 10 <> 5", Some(E::Where {
            left: E::IntLiteral(10).into(),
            operator: InfixOperator::NotEqual,
            right: E::IntLiteral(5).into()
        })),
        ("WHERE column", None),
        ("column <> other_column", None),
        ("WHERE * = 0", None),
    ];

    test_all_cases(Where, &inputs);
}

#[test]
fn array_basic() {
    let inputs = [
        ("(1, 2.3, 'hey', 4)", Some(E::Array(vec![
            E::IntLiteral(1),
            E::DecimalLiteral(2, 3),
            E::StrLiteral("hey".into()),
            E::IntLiteral(4),
        ]))),
        // Allow trailing commas
        ("(1,)", Some(E::Array(vec![
            E::IntLiteral(1),
        ]))),
    ];

    test_all_cases(Array, &inputs);
}
