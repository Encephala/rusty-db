#![allow(clippy::needless_return)]

pub mod parsers;


#[cfg(test)]
mod tests {
    use super::parsers::{
        Parser,
        Whitespace, Letter, Literal,
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
            .then(Literal::new(';').any());

        assert_eq!(parser.parse("SELECT * FROM blabla;".into()), Some(("SELECT * FROM blabla;".into(), "".into())));
        assert_eq!(parser.parse("SELECT asdf,fdsa FROM other".into()), Some(("SELECT asdf,fdsa FROM other".into(), "".into())));
        assert_eq!(parser.parse("SELECT *asdf FROM blabla;".into()), None);
    }
}
