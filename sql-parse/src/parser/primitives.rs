use super::super::lexer::Token;

struct Tokens<'a> {
    tokens: &'a mut [Token]
}

impl<'a> Tokens<'a> {
    fn new(value: &'a mut [Token]) -> Self {
        return Self { tokens: value };
    }

    fn drop_head(&'a mut self) {
        self.tokens = &mut self.tokens[1..];
    }
}

impl<'a, I: std::slice::SliceIndex<[Token]>> std::ops::Index<I> for Tokens<'a> {
    type Output = <I as std::slice::SliceIndex<[Token]>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        return &self.tokens[index];
    }
}

pub trait Parser {
    fn parse<'a>(input: Tokens<'a>) -> Option<Statement>;
}

fn check_and_skip<'a>(input: &'a mut Tokens<'a>, equals: Token) -> Option<()> {
    if input[0] != equals {
        return None;
    }

    input.drop_head();

    return Some(());
}

fn parse_identifier<'a>(input: &'a mut Tokens<'a>) -> Option<String> {
    // TODO: ensure length is >= 1
    let name = match &input[0] {
        Token::Ident(name) => Some(name.clone()),
        _ => None,
    }?;

    input.drop_head();

    return Some(name);
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    Select { column: String, table: String },
}


#[derive(Debug)]
struct SelectParser;

impl Parser for SelectParser {
    fn parse<'a>(mut input: Tokens<'a>) -> Option<Statement> {
        check_and_skip(&mut input, Token::Select)?;

        let column = parse_identifier(&mut input)?;

        check_and_skip(&mut input, Token::From)?;

        let table = parse_identifier(&mut input)?;

        // TODO: Where clause, etc.

        check_and_skip(&mut input, Token::Semicolon)?;

        return Some(Statement::Select {
            column,
            table,
        });
    }
}


#[cfg(test)]
mod tests {
    use crate::lexer::Lexer;

    use super::{Tokens, Parser, SelectParser, Statement};

    #[test]
    fn basic_select() {
        let input = "SELECT bla from asdf;";

        let mut tokens = Lexer::new(input).lex();

        let result = SelectParser::parse(Tokens::new(&mut tokens));

        if let Some(statement) = result {
            assert_eq!(
                statement,
                Statement::Select {
                    column: "bla".into(),
                    table: "asdf".into(),
                }
            );
        } else {
            panic!("Failed to parse SELECT");
        }
    }
}
