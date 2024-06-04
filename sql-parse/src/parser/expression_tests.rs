
use super::expressions::*;
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
            right: E::Int(5).into()
        })),
        ("WHERE column >= other_column", Some(E::Where {
            left: E::Ident("column".into()).into(),
            operator: InfixOperator::GreaterThanEqual,
            right: E::Ident("other_column".into()).into()
        })),
        ("WHERE 10 <> 5", Some(E::Where {
            left: E::Int(10).into(),
            operator: InfixOperator::NotEqual,
            right: E::Int(5).into()
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
            E::Int(1),
            E::Decimal(2, 3),
            E::Str("hey".into()),
            E::Int(4),
        ]))),
        // Allow trailing commas
        ("(1,)", Some(E::Array(vec![
            E::Int(1),
        ]))),
    ];

    test_all_cases(Array, &inputs);
}
