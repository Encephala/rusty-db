#![allow(clippy::needless_return)]

pub mod parser;
mod lexer;

pub fn parse(input: &str) {
    let _tokens = lexer::Lexer::new(input).lex();

    // TODO: Parsing
}

#[cfg(test)]
mod tests {
}
