#![allow(clippy::needless_return)]

pub mod lexer;
pub mod parser;

use lexer::{Lexer, Token};
use parser::statements::{StatementParser, Statement, Create, Insert, Select, Update, Delete, Drop};

pub fn parse_statement(input: &str) -> Option<Statement> {
    let tokens = &mut &Lexer::lex(input);

    return match tokens.first()? {
        Token::Create => Create.parse(tokens),
        Token::Insert => Insert.parse(tokens),
        Token::Select => Select.parse(tokens),
        Token::Update => Update.parse(tokens),
        Token::Delete => Delete.parse(tokens),
        Token::Drop => Drop.parse(tokens),
        _ => None,
    };
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_statements_basic() {
        let inputs = [
            ("SELECT * FROM blabla WHERE x = 5;"),
            ("CREATE TABLE blabla (a INT, b BOOl, c TEXT);"),
            ("INSERT INTO blabla (a, b, c) VALUES ('a', 'b', 'c');"),
            ("UPDATE tbl SET col1 = 1, col2 = 'bye' WHERE a = b;"),
            ("DELETE FROM tbl WHERE a = 5;"),
            ("DROP DATABASE db;"),
        ];

        inputs.iter().for_each(|test_case| {
            let result = parse_statement(test_case);

            assert!(result.is_some());
        })
    }
}
