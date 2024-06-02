use super::combinators::Chain;
use super::utils::check_and_skip;
use crate::lexer::Token;

#[derive(Debug, PartialEq)]
pub enum Expression {
    AllColumns,
    Ident(String),
    Int(usize),
    Where { left: Box<Expression>, operator: InfixOperator, right: Box<Expression> },
}
use Expression as E;


#[derive(Debug, PartialEq)]
pub enum InfixOperator {
    Equals,
    NotEqual,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
}

impl InfixOperator {
    fn parse(input: &mut &[Token]) -> Option<Self> {
        use InfixOperator as I;

        let operator = match input.get(0)? {
            Token::Equals => Some(I::Equals),
            Token::NotEquals => Some(I::NotEqual),
            Token::LessThan => Some(I::LessThan),
            Token::LessThanEqual => Some(I::LessThanEqual),
            Token::GreaterThan => Some(I::GreaterThan),
            Token::GreaterThanEqual => Some(I::GreaterThanEqual),
            _ => None,
        };

        if let Some(operator) = operator {
            *input = &input[1..];

            return Some(operator);
        }

        return None;
    }
}


pub trait Parser {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression>;
}

pub struct Number;
impl Parser for Number {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Some(Token::Int(value)) = input.get(0) {
            *input = &input[1..];

            return Some(E::Int(*value));
        }

        return None;
    }
}

pub struct Identifier;
impl Parser for Identifier {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        if let Token::Ident(name) = input.get(0)? {
            *input = &input[1..];

            return Some(E::Ident(name.clone()));
        }

        return None;
    }
}

pub struct AllColumn;
impl Parser for AllColumn {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::Asterisk)?;

        return Some(E::AllColumns);
    }
}

pub struct Column;
impl Parser for Column {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        return Identifier.or(AllColumn).parse(input);
    }
}

pub struct WhereParser;
impl Parser for WhereParser {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::Where)?;

        let parser = Column.or(Number);

        let left = parser.parse(input)?.into();

        let operator = InfixOperator::parse(input)?;

        let right = parser.parse(input)?.into();

        return Some(E::Where { left, operator, right });
    }
}


#[cfg(test)]
mod tests {
    use super::{AllColumn, Column, Expression, Identifier, InfixOperator, Number, Parser, WhereParser, E};
    use crate::lexer::Lexer;

    fn test_all_cases(parser: impl Parser, inputs: &[(&str, Option<Expression>)]) {
        inputs.iter().for_each(|test_case| {
            let result = parser.parse(&mut Lexer::new(test_case.0).lex().as_slice());

            assert_eq!(result, test_case.1);
        });
    }

    #[test]
    fn number_parser_basic() {
        let inputs = [
            ("1", Some(E::Int(1))),
            ("69420", Some(E::Int(69420))),
            ("asdf", None),
        ];

        test_all_cases(Number, &inputs);
    }

    #[test]
    fn identifier_parser_basic() {
        let inputs = [
            ("blablabla", Some(E::Ident("blablabla".into()))),
            ("Bl_a", Some(E::Ident("Bl_a".into()))),
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
            ("*", Some(E::AllColumns)),
            ("column_name", Some(E::Ident("column_name".into()))),
            ("otherColumnName", Some(E::Ident("otherColumnName".into()))),
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
        ];

        test_all_cases(WhereParser, &inputs);
    }
}
