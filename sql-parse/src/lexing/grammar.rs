use super::primitives::{Parser, Whitespace, Letter, Digit, Literal};
use super::combinators::Then;
use super::chaining::Chain;

/// Parses a literal string.
#[derive(Debug, Clone)]
pub struct Keyword {
    literal: String,
}

impl Parser for Keyword {
    fn parse(&self, input: String) -> Option<(String, String)> {
        if self.literal.is_empty() {
            return Some(("".into(), input));
        }

        let mut parser = Then::new(Literal::new(self.literal.chars().next().unwrap()));

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


/// Parses a single identifier.
/// An identifier is a column name or a table name.
#[derive(Debug, Clone)]
pub struct Identifier;

impl Parser for Identifier {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let parser = Letter.then(Letter.or(Digit).or(Literal::new('_')).any());

        return parser.parse(input);
    }
}


/// Parses a list of identifiers,
/// i.e. one or more identifiers, separated by a comma.
#[derive(Debug, Clone)]
pub struct IdentifierList;

impl Parser for IdentifierList {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let identifier_separator = Whitespace.any().then(Literal::new(',')).then(Whitespace.any());

        let parser = Identifier.then(identifier_separator.then(Identifier).any());

        return parser.parse(input);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_parser() {
        let parser = Keyword::new("SELECT");

        assert_eq!(parser.parse("SELECT".into()), Some(("SELECT".into(), "".into())));
        assert_eq!(parser.parse("garbage".into()), None);
    }

    #[test]
    fn test_keyword_parser_empty() {
        let parser = Keyword::new("");

        assert_eq!(parser.parse("".into()), Some(("".into(), "".into())));
        assert_eq!(parser.parse("garbage".into()), Some(("".into(), "garbage".into())));
    }

    #[test]
    fn test_identifier_parser() {
        let parser = Identifier;

        assert_eq!(parser.parse("column".into()), Some(("column".into(), "".into())));
        assert_eq!(parser.parse("table_1".into()), Some(("table_1".into(), "".into())));
        assert_eq!(parser.parse("123".into()), None);
        assert_eq!(parser.parse("_underscore".into()), None);
    }

    #[test]
    fn test_identifier_list_parser() {
        let parser = IdentifierList;

        assert_eq!(parser.parse("column1, column2 , column3,column4".into()), Some(("column1, column2 , column3,column4".into(), "".into())));
    }
}
