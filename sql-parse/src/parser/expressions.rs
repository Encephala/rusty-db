use crate::lexer::Token;
use super::combinators::{Chain, Or};
use super::utils::check_and_skip;

#[derive(Debug, PartialEq)]
pub enum Expression {
    AllColumns,
    Ident(String),
}
use Expression as E;

pub trait Parser {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression>;
}


pub struct IdentifierParser;
impl Parser for IdentifierParser {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        // TODO: ensure length is >= 1
        let name = match &input[0] {
            Token::Ident(name) => Some(name.clone()),
            _ => None,
        }?;

        *input = &mut &input[1..];

        return Some(E::Ident(name));
    }
}

pub struct WhereParser;
impl Parser for WhereParser {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        check_and_skip(input, Token::Where)?;

        todo!()
    }
}

pub struct AllColumnParser;
impl Parser for AllColumnParser {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        check_and_skip(input, Token::Asterisk)?;

        return Some(E::AllColumns);
    }
}

pub struct ColumnParser;
impl Parser for ColumnParser {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        return IdentifierParser.or(AllColumnParser).parse(input);
    }
}


fn parse_identifier(input: &mut &[Token]) -> Option<Expression> {
    // TODO: ensure length is >= 1
    let name = match &input[0] {
        Token::Ident(name) => Some(name.clone()),
        _ => None,
    }?;

    *input = &mut &input[1..];

    return Some(E::Ident(name));
}

fn parse_expression(input: &mut &[Token]) -> Option<Expression> {
    // TODO: Parsing expressions recursively or something idk who cares

    // let left =

    todo!();
}

#[cfg(test)]
mod tests {
    use crate::lexer::{Token, Lexer};
    use super::*;

    macro_rules! test_case {
        ($type:ty) => {
            struct TestCase<'a>(&'a str, $type);
        };
    }

    #[test]
    fn column_parser_basic_functionality() {
        test_case!(Option<Expression>);

        let inputs = [
            TestCase("*", Some(E::AllColumns)),
            TestCase("column_name", Some(E::Ident("column_name".into()))),
            TestCase("otherColumnName", Some(E::Ident("otherColumnName".into()))),
        ];

        inputs.iter().for_each(|test_case| {
            let result = ColumnParser.parse(
                &mut Lexer::new(test_case.0).lex().as_slice()
            );

            assert_eq!(result, test_case.1);
        });
    }
}
