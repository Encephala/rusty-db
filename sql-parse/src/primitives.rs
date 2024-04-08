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
pub trait Primitive: Debug + DynClone {
    fn parse(&self, input: String) -> Option<(String, String)>;
}

dyn_clone::clone_trait_object!(Primitive);


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
impl Primitive for Whitespace {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_whitespace())
    }
}


/// Parses a letter character, as defined by the [`char::is_alphabetic`] method.
#[derive(Debug, Clone)]
pub struct Letter;
impl Primitive for Letter {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_alphabetic())
    }
}


/// Parses a digit character, as defined by the [`char::is_ascii_digit`] method.
#[derive(Debug, Clone)]
pub struct Digit;
impl Primitive for Digit {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_ascii_digit())
    }
}


/// Parses a given specific character.
#[derive(Debug, Clone)]
pub struct Literal {
    literal: char,
}

impl Primitive for Literal {
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
impl Primitive for Empty {
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
    fn test_whitespace_Primitive() {
        let Primitive = Whitespace;

        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(Primitive.parse("a".into()), None);
    }

    #[test]
    fn test_whitespace_Primitive_unicode_whitespace() {
        let Primitive = Whitespace;

        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse("\t".into()), Some(("\t".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(Primitive.parse(" ".into()), Some((" ".into(), "".into())));
    }

    #[test]
    fn test_letter_Primitive() {
        let Primitive = Letter;

        assert_eq!(Primitive.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(Primitive.parse("A".into()), Some(("A".into(), "".into())));
        assert_eq!(Primitive.parse("1".into()), None);
        assert_eq!(Primitive.parse(" ".into()), None);
    }

    #[test]
    fn test_digit_Primitive() {
        let Primitive = Digit;

        assert_eq!(Primitive.parse("1".into()), Some(("1".into(), "".into())));
        assert_eq!(Primitive.parse("12".into()), Some(("1".into(), "2".into())));
        assert_eq!(Primitive.parse("a".into()), None);
    }

    #[test]
    fn test_literal_Primitive() {
        let Primitive = Literal { literal: 'a' };

        assert_eq!(Primitive.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(Primitive.parse("b".into()), None);
    }
}
