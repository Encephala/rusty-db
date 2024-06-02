#![allow(clippy::needless_return)]

mod lexer;
pub mod parser;

pub fn parse(input: &str) {
    let _tokens = lexer::Lexer::new(input).lex();

    // TODO: Parsing
}

#[cfg(test)]
mod tests {}
