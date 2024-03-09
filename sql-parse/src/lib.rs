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
        let parser = Keyword { literal: "SELECT".into() }
            .then(Whitespace.all())
            .then(Letter.all().or(Literal { literal: '*' })
                .then(Literal { literal: ',' }.then(Letter.all()).any())
            )
            .then(Whitespace.all())
            .then(Keyword { literal: "FROM".into() })
            .then(Whitespace.all())
            .then(Letter.all())
            .then(Literal { literal: ';' }.any());

        assert_eq!(parser.parse("SELECT * FROM blabla;".into()), Some(("SELECT * FROM blabla;".into(), "".into())));
        assert_eq!(parser.parse("SELECT asdf,fdsa FROM other".into()), Some(("SELECT asdf,fdsa FROM other".into(), "".into())));
        assert_eq!(parser.parse("SELECT *asdf FROM blabla;".into()), None);
    }
}
