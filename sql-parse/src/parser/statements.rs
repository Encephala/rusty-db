use super::expressions::{Expression, ExpressionParser, Identifier, Where, Array};
use super::utils::check_and_skip;
use crate::lexer::Token;

#[derive(Debug, PartialEq)]
pub enum Statement {
    Select {
        column: Expression,
        table: Expression,
        where_clause: Option<Expression>,
    },
    Create {
        what: CreateType,
        name: Expression,
    },
    Insert {
        into: Expression,
        values: Expression, // Expression::Array
    },
}

#[derive(Debug, PartialEq)]
pub enum CreateType {
    Database,
    Table,
}

pub trait StatementParser {
    fn parse(&self, input: &[Token]) -> Option<Statement>;
}


pub struct Select;
impl StatementParser for Select {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Select)?;

        let column = Identifier.parse(input)?;

        check_and_skip(input, Token::From)?;

        let table = Identifier.parse(input)?;

        let where_clause = Where.parse(input);

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Select {
            column,
            table,
            where_clause,
        });
    }
}


pub struct Create;
impl StatementParser for Create {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Create)?;

        let creation_type = match input.get(0)? {
            Token::Table => Some(CreateType::Table),
            Token::Database => Some(CreateType::Database),
            _ => None,
        }?;

        *input = &input[1..];

        let name = Identifier.parse(input)?;

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Create {
            what: creation_type,
            name
        });
    }
}


pub struct Insert;
impl StatementParser for Insert {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Insert)?;

        check_and_skip(input, Token::Into)?;

        let into = Identifier.parse(input)?;

        check_and_skip(input, Token::Values)?;

        let values = Array.parse(input)?;

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Insert {
           into,
           values
        });
    }
}


#[cfg(test)]
mod tests {
    use crate::lexer::Lexer;
    use super::super::expressions::InfixOperator;
    use super::*;

    use {Expression as E, Statement as S};


    pub fn test_all_cases(parser: impl StatementParser, inputs: &[(&str, Option<Statement>)]) {
        inputs.iter().for_each(|test_case| {
            let result = parser.parse(Lexer::lex(test_case.0).as_slice());

            assert_eq!(result, test_case.1);
        });
    }


    #[test]
    fn select_basic() {
        let input = "SELECT bla from asdf;";

        let tokens = Lexer::lex(input);

        let result = Select.parse(tokens.as_slice());

        assert_eq!(
            result,
            Some(S::Select {
                column: E::Ident("bla".into()),
                table: E::Ident("asdf".into()),
                where_clause: None,
            })
        );
    }

    #[test]
    fn select_with_where() {
        let input = "SELECT bla FROM asdf WHERE a > b;";

        let result = Select.parse(Lexer::lex(input).as_slice());

        assert_eq!(
            result,
            Some(S::Select {
                column: E::Ident("bla".into()),
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
            })),
            ("CREATE TABLE name;", Some(S::Create{
                what: CreateType::Table,
                name: E::Ident("name".into()),
            })),
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
}
