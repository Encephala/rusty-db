//! Combinators for combining lexers together.
//! - [`All`]: Matches one or more matches of the given lexer.
//! - [`Any`]: Matches zero or more matches of the given lexer.
//! - [`Or`]: Matches the first match of any of the given lexers.
//! - [`Then`]: Matches the first lexer, then the second lexer.

use super::primitives::Tokeniser;

/// Matches one or more matches of the given lexer.
#[derive(Debug, Clone)]
pub struct All {
    lexer: Box<dyn Tokeniser>,
}

impl All {
    pub fn new(lexer: impl Tokeniser + 'static) -> Self {
        return All { lexer: Box::new(lexer) };
    }
}

impl Tokeniser for All {
    fn consume(&self, input: String) -> Option<(String, String)> {
        let mut match_length = 0;
        let mut remainder = input.clone();

        while let Some((matched, new_remainder)) = self.lexer.consume(remainder) {
            remainder = new_remainder;
            match_length += matched.chars().map(|c| c.len_utf8()).sum::<usize>();
        }

        if match_length == 0 {
            return None;
        }

        return Some((input[..match_length].into(), input[match_length..].into()));
    }
}


/// Matches zero or more matches of the given lexer.
#[derive(Debug, Clone)]
pub struct Any {
    lexer: Box<dyn Tokeniser>
}

impl Any {
    pub fn new(lexer: impl Tokeniser + 'static) -> Self {
        return Any { lexer: Box::new(lexer) };
    }
}

impl Tokeniser for Any {
    fn consume(&self, input: String) -> Option<(String, String)> {
        let mut match_length = 0;
        let mut remainder = input.clone();

        while let Some((matched, new_remainder)) = self.lexer.consume(remainder) {
            remainder = new_remainder;
            match_length += matched.chars().map(|c| c.len_utf8()).sum::<usize>();
        }

        return Some((input[..match_length].into(), input[match_length..].into()));
    }
}


/// Matches the first match of any of the given lexers.

// TODO: Is this a problem due to ambiguous grammars
// in that the order of the lexers matters?
#[derive(Debug, Clone)]
pub struct Or {
    lexers: Vec<Box<dyn Tokeniser>>,
}

impl Or {
    pub fn new(lexer: impl Tokeniser + 'static) -> Self {
        return Or { lexers: vec![Box::new(lexer)] };
    }

    pub fn or(mut self, lexer: impl Tokeniser + 'static) -> Self {
        self.lexers.push(Box::new(lexer));
        return self;
    }
}

impl Tokeniser for Or {
    fn consume(&self, input: String) -> Option<(String, String)> {
        for lexer in &self.lexers {
            if let Some((matched, remainder)) = lexer.consume(input.clone()) {
                return Some((matched, remainder));
            }
        }

        return None;
    }
}


/// Matches the first lexer, then the second lexer.
#[derive(Debug, Clone)]
pub struct Then {
    lexer: Box<dyn Tokeniser>,
    next: Option<Box<dyn Tokeniser>>,
}

impl Then {
    pub fn new(lexer: impl Tokeniser + 'static) -> Self {
        return Then { lexer: Box::new(lexer), next: None };
    }
}

impl Tokeniser for Then {
    fn consume(&self, input: String) -> Option<(String, String)> {
        // TODO: It's not great that this returns None if self.then is None
        return self.lexer.consume(input).and_then(|(matched, remainder)| {
            self.next.as_ref()?
                .consume(remainder)
                .map(|(then_matched, remainder)| (matched + &then_matched, remainder))
        });
    }
}

impl Then {
    pub fn then(mut self, lexer: impl Tokeniser + 'static) -> Self {
        // Base case
        if self.next.is_none() {
            self.next = Some(Box::new(lexer));

            return self;
        }

        // General case
        self.next = Some(Box::new(Then {
            lexer: self.next.take().unwrap(),
            next: Some(Box::new(lexer))
        }));

        return self;
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::super::primitives::*;
    use super::super::chaining::*;

    #[test]
    fn test_all_combinator() {
        let lexer = Whitespace.all();

        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(lexer.consume("a".into()), None);
        assert_eq!(lexer.consume("  ".into()), Some(("  ".into(), "".into())));
        assert_eq!(lexer.consume(" a ".into()), Some((" ".into(), "a ".into())));
        assert_eq!(lexer.consume("a ".into()), None);
        assert_eq!(lexer.consume(" \t           asdf".into()), Some((" \t           ".into(), "asdf".into())));
    }

    #[test]
    fn test_any_combinator() {
        let lexer = Whitespace.any();

        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(lexer.consume("a".into()), Some(("".into(), "a".into())));
        assert_eq!(lexer.consume("  ".into()), Some(("  ".into(), "".into())));
        assert_eq!(lexer.consume(" a ".into()), Some((" ".into(), "a ".into())));
        assert_eq!(lexer.consume("a ".into()), Some(("".into(), "a ".into())));
        assert_eq!(lexer.consume(" \t           asdf".into()), Some((" \t           ".into(), "asdf".into())));
    }

    #[test]
    fn test_or_combinator() {
        let lexer = Whitespace.or(Letter);

        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume("a".into()), Some(("a".into(), "".into())));
        assert_eq!(lexer.consume(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(lexer.consume("  ".into()), Some((" ".into(), " ".into())));
        assert_eq!(lexer.consume("1 ".into()), None);

        let lexer = lexer.or(Digit);

        assert_eq!(lexer.consume(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(lexer.consume("a".into()), Some(("a".into(), "".into())));
        assert_eq!(lexer.consume("1".into()), Some(("1".into(), "".into())));
        assert_eq!(lexer.consume(" a".into()), Some((" ".into(), "a".into())));
    }

    #[test]
    fn test_then_combinator() {
        let lexer = Digit.then(Letter);

        assert_eq!(lexer.consume("1".into()), None);
        assert_eq!(lexer.consume("1a".into()), Some(("1a".into(), "".into())));
        assert_eq!(lexer.consume("a1".into()), None);
        assert_eq!(lexer.consume("11".into()), None);
        assert_eq!(lexer.consume(" a".into()), None);
    }

    #[test]
    fn then_returns_none_if_next_is_none() {
        let lexer = Then::new(Digit);

        assert_eq!(lexer.consume("1a".into()), None);
    }

    #[test]
    fn test_combining_combinators() {
        let lexer = Whitespace.all().then(
            Letter.or(Digit)
                .or(Literal::new('<'))
            );

        assert_eq!(lexer.consume(" ".into()), None);
        assert_eq!(lexer.consume("a1".into()), None);
        assert_eq!(lexer.consume("  ".into()), None);
        assert_eq!(lexer.consume(" 1a".into()), Some((" 1".into(), "a".into())));
        assert_eq!(lexer.consume(" <1".into()), Some((" <".into(), "1".into())));
    }
}
