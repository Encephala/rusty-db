#![allow(clippy::needless_return)]

pub mod parsing;
pub mod lexing;


#[cfg(test)]
mod tests {
    use super::lexing::{
        Tokeniser,
        Whitespace, Letter, Literal, Empty,
        Word,
        Chain,
    };

    #[test]
    fn parse_basic_select_statement() {
        let parser = Word::new("SELECT")
            .then(Whitespace.all())
            .then(Letter.all().or(Literal::new('*'))
                .then(Literal::new(',').then(Letter.all()).any())
            )
            .then(Whitespace.all())
            .then(Word::new("FROM"))
            .then(Whitespace.all())
            .then(Letter.all())
            .then(Empty.or(Literal::new(';')));

        assert_eq!(parser.parse("SELECT * FROM blabla;".into()), Some(("SELECT * FROM blabla;".into(), "".into())));
        assert_eq!(parser.parse("SELECT asdf,fdsa FROM other".into()), Some(("SELECT asdf,fdsa FROM other".into(), "".into())));
        assert_eq!(parser.parse("SELECT *asdf FROM blabla;".into()), None);
    }
}
