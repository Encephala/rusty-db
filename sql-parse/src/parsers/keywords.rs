use super::primitives::{Parser, Literal};
use super::combinators::Then;

#[derive(Debug, Clone)]
pub struct Keyword {
    literal: String,
}

impl Parser for Keyword {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let mut parser: Then = Then::new(Literal::new(self.literal.chars().next().unwrap()));

        for c in self.literal.chars().skip(1) {
            parser = parser.then(Literal::new(c));
        }

        return parser.parse(input);
    }
}

impl Keyword {
    pub fn new(literal: &str) -> Self {
        return Keyword { literal: literal.into() };
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_parser() {
        let parser = Keyword::new("SELECT");

        assert_eq!(parser.parse("SELECT".into()), Some(("SELECT".into(), "".into())));
        assert_eq!(parser.parse("SELECT *".into()), Some(("SELECT".into(), " *".into())));
        assert_eq!(parser.parse("SELEC".into()), None);
        assert_eq!(parser.parse("garbage".into()), None);
    }
}
