//! Primitives for parsing individual characters.
//! - [`Whitespace`]: Matches a whitespace character.
//! - [`Letter`]: Matches a roman letter character.
//! - [`Digit`]: Matches a digit character.
//! - [`Literal`]: Matches a given specific character.

use core::fmt::Debug;
use dyn_clone::DynClone;

/// Matches the input string by some rule.
///
/// If the input string conforms the rule, it returns the matched string and the remaining string.
/// Otherwise, it returns [`None`].
pub trait Tokeniser: Debug + DynClone {
    fn consume(&self, input: String) -> Option<(String, String)>;
}

dyn_clone::clone_trait_object!(Tokeniser);


fn parse_if<F>(input: String, predicate: F) -> Option<(String, String)>
where F: Fn(char) -> bool {
    let condition = input.chars().next().map(predicate)?;

    if condition {
        let index_second_char = input.chars().next().map(|c| c.len_utf8())?;

        return Some((input[..index_second_char].into(), input[index_second_char..].into()));
    }

    return None;
}


/// Matches a whitespace character, as defined by the [`char::is_whitespace`] method.
#[derive(Debug, Clone)]
pub struct Whitespace;
impl Tokeniser for Whitespace {
    fn consume(&self, input: String) -> Option<(String, String)> {
        return parse_if(input, |c| c.is_whitespace());
    }
}


/// Matches a letter character, as defined by the [`char::is_alphabetic`] method.
#[derive(Debug, Clone)]
pub struct Letter;
impl Tokeniser for Letter {
    fn consume(&self, input: String) -> Option<(String, String)> {
        return parse_if(input, |c| c.is_alphabetic());
    }
}


/// Matches a digit character, as defined by the [`char::is_ascii_digit`] method.
#[derive(Debug, Clone)]
pub struct Digit;
impl Tokeniser for Digit {
    fn consume(&self, input: String) -> Option<(String, String)> {
        return parse_if(input, |c| c.is_ascii_digit());
    }
}


/// Matches a given specific character.
#[derive(Debug, Clone)]
pub struct Literal {
    literal: char,
}

impl Tokeniser for Literal {
    fn consume(&self, input: String) -> Option<(String, String)> {
        return parse_if(input, |c| c == self.literal);
    }
}

impl Literal {
    pub fn new(literal: char) -> Self {
        return Literal { literal };
    }
}


/// Matches that the input is empty (i.e. the end of the input).
#[derive(Debug, Clone)]
pub struct Empty;
impl Tokeniser for Empty {
    fn consume(&self, input: String) -> Option<(String, String)> {
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
    fn test_whitespace_lexer() {
        let lexer = Whitespace;

        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(lexer.consume("a".into()), None);
    }

    #[test]
    fn test_whitespace_lexer_unicode_whitespace() {
        let lexer = Whitespace;

        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume("\t".into()), Some(("\t".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
    }

    #[test]
    fn test_letter_lexer() {
        let lexer = Letter;

        assert_eq!(lexer.consume("a".into()), Some(("a".into(), "".into())));
        assert_eq!(lexer.consume("A".into()), Some(("A".into(), "".into())));
        assert_eq!(lexer.consume("1".into()), None);
        assert_eq!(lexer.consume(" ".into()), None);
    }

    #[test]
    fn test_digit_lexer() {
        let lexer = Digit;

        assert_eq!(lexer.consume("1".into()), Some(("1".into(), "".into())));
        assert_eq!(lexer.consume("12".into()), Some(("1".into(), "2".into())));
        assert_eq!(lexer.consume("a".into()), None);
    }

    #[test]
    fn test_literal_lexer() {
        let lexer = Literal { literal: 'a' };

        assert_eq!(lexer.consume("a".into()), Some(("a".into(), "".into())));
        assert_eq!(lexer.consume("b".into()), None);
    }
}
