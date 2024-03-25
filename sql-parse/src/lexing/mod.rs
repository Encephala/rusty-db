mod tokens;
mod primitives;
mod combinators;
mod chaining;
mod grammar;

pub use primitives::{Tokeniser, Whitespace, Digit, Letter, Literal, Empty};
pub use combinators::{All, Any, Or, Then};
pub use chaining::Chain;


/// Matches a literal string.
#[derive(Debug, Clone)]
pub struct Keyword {
    literal: String,
}

impl Lexer for Keyword {
    fn parse(&self, input: String) -> Option<(String, String)> {
        if self.literal.is_empty() {
            return Some(("".into(), input));
        }

        let mut lexer = Then::new(Literal::new(self.literal.chars().next().unwrap()));

        for c in self.literal.chars().skip(1) {
            lexer = lexer.then(Literal::new(c));
        }

        return lexer.parse(input);
    }
}

impl Keyword {
    pub fn new(literal: &str) -> Self {
        return Keyword { literal: literal.into() };
    }
}


/// Matches a single identifier.
/// An identifier is a column name or a table name.
#[derive(Debug, Clone)]
pub struct Identifier;

impl Lexer for Identifier {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let lexer = Letter.then(Letter.or(Digit).or(Literal::new('_')).any());

        return lexer.parse(input);
    }
}


/// Matches a list of identifiers,
/// i.e. one or more identifiers, separated by a comma.
#[derive(Debug, Clone)]
pub struct IdentifierList;

impl Lexer for IdentifierList {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let identifier_separator = Whitespace.any().then(Literal::new(',')).then(Whitespace.any());

        let lexer = Identifier.then(identifier_separator.then(Identifier).any());

        return lexer.parse(input);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_lexer() {
        let lexer = Keyword::new("SELECT");

        assert_eq!(lexer.parse("SELECT".into()), Some(("SELECT".into(), "".into())));
        assert_eq!(lexer.parse("garbage".into()), None);
    }

    #[test]
    fn test_keyword_lexer_empty() {
        let lexer = Keyword::new("");

        assert_eq!(lexer.parse("".into()), Some(("".into(), "".into())));
        assert_eq!(lexer.parse("garbage".into()), Some(("".into(), "garbage".into())));
    }

    #[test]
    fn test_identifier_lexer() {
        let lexer = Identifier;

        assert_eq!(lexer.parse("column".into()), Some(("column".into(), "".into())));
        assert_eq!(lexer.parse("table_1".into()), Some(("table_1".into(), "".into())));
        assert_eq!(lexer.parse("123".into()), None);
        assert_eq!(lexer.parse("_underscore".into()), None);
    }

    #[test]
    fn test_identifier_list_lexer() {
        let lexer = IdentifierList;

        assert_eq!(lexer.parse("column1, column2 , column3,column4".into()), Some(("column1, column2 , column3,column4".into(), "".into())));
    }
}
