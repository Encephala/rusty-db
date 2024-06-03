#![allow(clippy::needless_return)]

mod lexer;
pub mod parser;

use lexer::{Token, Lexer};
use parser::{Statement, StatementParser, Create, Insert, Select};

pub fn parse_statement(input: &str) -> Option<Statement> {
    let tokens = &mut &Lexer::lex(input);

    return match tokens.get(0)? {
        Token::Create => Create.parse(tokens),
        Token::Insert => Insert.parse(tokens),
        Token::Select => Select.parse(tokens),
        _ => None,
    };
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_statements_basic() {
        let inputs = [
            ("SELECT * FROM blabla;"),
            ("SELECT * FROM blabla WHERE x = 5;"),
            ("CREATE TABLE blabla;"),
            ("CREATE DATABASE xd;"),
            ("INSERT INTO blabla VALUES ('a', 'b', 'c');"),
        ];

        inputs.iter().for_each(|test_case| {
            let result = parse_statement(test_case);

            dbg!(*test_case);
            assert!(result.is_some());
        })
    }
}
