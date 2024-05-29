#![allow(clippy::needless_return)]

pub mod parser;
mod lexer;

pub fn parse(input: &str) {
    let _tokens = lexer::Lexer::new(input).lex();

    // TODO: Parsing
}

#[cfg(test)]
mod tests {
    use super::parser::{
        Parser,
        Whitespace, Letter, Literal, Empty,
        Keyword,
        Chain,
    };

    #[test]
    fn parse_basic_select_statement() {
        let parser = Keyword::new("SELECT")
            .then(Whitespace.all())
            .then(Letter.all().or(Literal::new('*'))
                .then(Literal::new(',').then(Letter.all()).any())
            )
            .then(Whitespace.all())
            .then(Keyword::new("FROM"))
            .then(Whitespace.all())
            .then(Letter.all())
            .then(Empty.or(Literal::new(';')));

        assert_eq!(parser.parse("SELECT * FROM blabla;".into()), Some(("SELECT * FROM blabla;".into(), "".into())));
        assert_eq!(parser.parse("SELECT asdf,fdsa FROM other".into()), Some(("SELECT asdf,fdsa FROM other".into(), "".into())));
        assert_eq!(parser.parse("SELECT *asdf FROM blabla;".into()), None);
    }
}
