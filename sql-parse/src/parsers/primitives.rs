//! Primitives for parsing individual characters.
//! - [`Whitespace`]: Parses a whitespace character.
//! - [`Letter`]: Parses a roman letter character.
//! - [`Digit`]: Parses a digit character.
//! - [`Literal`]: Parses a given specific character.

use core::fmt::Debug;
use dyn_clone::DynClone;

/// Parses the input string by some rule.
///
/// If the input string conforms the rule, it returns the matched string and the remaining string.
/// Otherwise, it returns [`None`].
pub trait Parser: Debug + DynClone {
    fn parse(&self, input: String) -> Option<(String, String)>;
}

dyn_clone::clone_trait_object!(Parser);


fn parse_if<F>(input: String, predicate: F) -> Option<(String, String)>
where F: Fn(char) -> bool {
    let condition = input.chars().next().map(predicate)?;

    if condition {
        let index_second_char = input.chars().next().map(|c| c.len_utf8())?;

        return Some((input[..index_second_char].into(), input[index_second_char..].into()));
    }

    return None;
}


/// Parses a whitespace character, as defined by the [`char::is_whitespace`] method.
#[derive(Debug, Clone)]
pub struct Whitespace;
impl Parser for Whitespace {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_whitespace())
    }
}


/// Parses a letter character, as defined by the [`char::is_alphabetic`] method.
#[derive(Debug, Clone)]
pub struct Letter;
impl Parser for Letter {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_alphabetic())
    }
}


/// Parses a digit character, as defined by the [`char::is_ascii_digit`] method.
#[derive(Debug, Clone)]
pub struct Digit;
impl Parser for Digit {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_ascii_digit())
    }
}


/// Parses a given specific character.
#[derive(Debug, Clone)]
pub struct Literal {
    literal: char,
}

impl Parser for Literal {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c == self.literal)
    }
}

impl Literal {
    pub fn new(literal: char) -> Self {
        return Literal { literal };
    }
}


/// Parses that the input is empty (i.e. the end of the input).
#[derive(Debug, Clone)]
pub struct Empty;
impl Parser for Empty {
    fn parse(&self, input: String) -> Option<(String, String)> {
        if input.is_empty() {
            return Some(("".into(), "".into()));
        }

        return None;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_whitespace_parser() {
        let parser = Whitespace;

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(parser.parse("a".into()), None);
    }

    #[test]
    fn test_whitespace_parser_all_unicode_whitespace() {
        let parser = Whitespace;

        assert_eq!(parser.parse(" \t           asdf".into()), Some((" ".into(), "\t           asdf".into())));
        assert_eq!(parser.parse("asdf".into()), None);
    }

    #[test]
    fn test_letter_parser() {
        let parser = Letter;

        assert_eq!(parser.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(parser.parse("A".into()), Some(("A".into(), "".into())));
        assert_eq!(parser.parse("1".into()), None);
        assert_eq!(parser.parse(" ".into()), None);
    }

    #[test]
    fn test_digit_parser() {
        let parser = Digit;

        assert_eq!(parser.parse("1".into()), Some(("1".into(), "".into())));
        assert_eq!(parser.parse("12".into()), Some(("1".into(), "2".into())));
        assert_eq!(parser.parse("a".into()), None);
    }

    #[test]
    fn test_literal_parser() {
        let parser = Literal { literal: 'a' };

        assert_eq!(parser.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(parser.parse("b".into()), None);
    }
}
