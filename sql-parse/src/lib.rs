#![allow(clippy::needless_return)]

mod lexer;
pub mod parser;

use lexer::{Token, Lexer};
use parser::{Statement, StatementParser, Create, Insert, Select, Update, Delete};

pub use parser::{Expression, ColumnType};

pub fn parse_statement(input: &str) -> Option<Statement> {
    let tokens = &mut &Lexer::lex(input);

    return match tokens.get(0)? {
        Token::Create => Create.parse(tokens),
        Token::Insert => Insert.parse(tokens),
        Token::Select => Select.parse(tokens),
        Token::Update => Update.parse(tokens),
        Token::Delete => Delete.parse(tokens),
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
            ("CREATE TABLE blabla (INT, BOOl, VARCHAR(10));"),
            ("INSERT INTO blabla VALUES ('a', 'b', 'c');"),
            ("UPDATE tbl SET col1 = 1, col2 = 'bye' WHERE a = b;"),
            ("DELETE FROM tbl WHERE a = 5;"),
        ];

        inputs.iter().for_each(|test_case| {
            let result = parse_statement(test_case);

            dbg!(*test_case);
            assert!(result.is_some());
        })
    }
}
