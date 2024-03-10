//! Combinators for combining parsers together.
//! - [`All`]: Parses one or more matches of the given parser.
//! - [`Any`]: Parses zero or more matches of the given parser.
//! - [`Or`]: Parses the first match of any of the given parsers.
//! - [`Then`]: Parses the first parser, then the second parser.

use super::primitives::Parser;

/// Parses one or more matches of the given parser.
#[derive(Debug, Clone)]
pub struct All {
    parser: Box<dyn Parser>,
}

impl All {
    pub fn new(parser: impl Parser + 'static) -> Self {
        return All { parser: Box::new(parser) };
    }
}

impl Parser for All {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let mut match_length = 0;
        let mut remainder = input.clone();

        while let Some((matched, new_remainder)) = self.parser.parse(remainder) {
            remainder = new_remainder;
            match_length += matched.chars().map(|c| c.len_utf8()).sum::<usize>();
        }

        if match_length == 0 {
            return None;
        }

        return Some((input[..match_length].into(), input[match_length..].into()));
    }
}


/// Parses zero or more matches of the given parser.
#[derive(Debug, Clone)]
pub struct Any {
    parser: Box<dyn Parser>
}

impl Any {
    pub fn new(parser: impl Parser + 'static) -> Self {
        return Any { parser: Box::new(parser) };
    }
}

impl Parser for Any {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let mut match_length = 0;
        let mut remainder = input.clone();

        while let Some((matched, new_remainder)) = self.parser.parse(remainder) {
            remainder = new_remainder;
            match_length += matched.chars().map(|c| c.len_utf8()).sum::<usize>();
        }

        return Some((input[..match_length].into(), input[match_length..].into()));
    }
}


/// Parses the first match of any of the given parsers.

// TODO: Is this a problem due to ambiguous grammars
// in that the order of the parsers matters?
#[derive(Debug, Clone)]
pub struct Or {
    parsers: Vec<Box<dyn Parser>>,
}

impl Or {
    pub fn new(parser: impl Parser + 'static) -> Self {
        return Or { parsers: vec![Box::new(parser)] };
    }

    pub fn or(mut self, parser: impl Parser + 'static) -> Self {
        self.parsers.push(Box::new(parser));
        return self;
    }
}

impl Parser for Or {
    fn parse(&self, input: String) -> Option<(String, String)> {
        for parser in &self.parsers {
            if let Some((matched, remainder)) = parser.parse(input.clone()) {
                return Some((matched, remainder));
            }
        }

        return None;
    }
}


/// Parses the first parser, then the second parser.
#[derive(Debug, Clone)]
pub struct Then {
    parser: Box<dyn Parser>,
    next: Option<Box<dyn Parser>>,
}

impl Then {
    pub fn new(parser: impl Parser + 'static) -> Self {
        return Then { parser: Box::new(parser), next: None };
    }
}

impl Parser for Then {
    fn parse(&self, input: String) -> Option<(String, String)> {
        // TODO: It's not great that this returns None if self.then is None
        return self.parser.parse(input).and_then(|(matched, remainder)| {
            self.next.as_ref()?
                .parse(remainder)
                .map(|(then_matched, remainder)| (matched + &then_matched, remainder))
        });
    }
}

impl Then {
    pub fn then(mut self, parser: impl Parser + 'static) -> Self {
        // Base case
        if self.next.is_none() {
            self.next = Some(Box::new(parser));

            return self;
        }

        // General case
        self.next = Some(Box::new(Then {
            parser: self.next.take().unwrap(),
            next: Some(Box::new(parser))
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
        let parser = All::new(Whitespace);

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(parser.parse("a".into()), None);
        assert_eq!(parser.parse("  ".into()), Some(("  ".into(), "".into())));
        assert_eq!(parser.parse(" a ".into()), Some((" ".into(), "a ".into())));
        assert_eq!(parser.parse("a ".into()), None);
        assert_eq!(parser.parse(" \t           asdf".into()), Some((" \t           ".into(), "asdf".into())));
    }

    #[test]
    fn test_any_combinator() {
        let parser = Any::new(Whitespace);

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(parser.parse("a".into()), Some(("".into(), "a".into())));
        assert_eq!(parser.parse("  ".into()), Some(("  ".into(), "".into())));
        assert_eq!(parser.parse(" a ".into()), Some((" ".into(), "a ".into())));
        assert_eq!(parser.parse("a ".into()), Some(("".into(), "a ".into())));
        assert_eq!(parser.parse(" \t           asdf".into()), Some((" \t           ".into(), "asdf".into())));
    }

    #[test]
    fn test_or_combinator() {
        let parser = Or::new(Whitespace)
            .or(Letter);

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(parser.parse("  ".into()), Some((" ".into(), " ".into())));
        assert_eq!(parser.parse("1 ".into()), None);

        let parser = parser.or(Digit);

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(parser.parse("1".into()), Some(("1".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
    }

    #[test]
    fn test_then_combinator() {
        let parser = Then::new(Digit).then(Letter);

        assert_eq!(parser.parse("1".into()), None);
        assert_eq!(parser.parse("1a".into()), Some(("1a".into(), "".into())));
        assert_eq!(parser.parse("a1".into()), None);
        assert_eq!(parser.parse("11".into()), None);
        assert_eq!(parser.parse(" a".into()), None);
    }

    #[test]
    fn test_combining_combinators() {
        let parser = Whitespace.all().then(
            Letter.or(Digit)
                .or(Literal::new('<'))
            );

        assert_eq!(parser.parse(" ".into()), None);
        assert_eq!(parser.parse("a1".into()), None);
        assert_eq!(parser.parse("  ".into()), None);
        assert_eq!(parser.parse(" 1a".into()), Some((" 1".into(), "a".into())));
        assert_eq!(parser.parse(" <1".into()), Some((" <".into(), "1".into())));
    }
}
