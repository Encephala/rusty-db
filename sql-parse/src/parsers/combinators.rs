use super::primitives::Parser;

pub trait Combinator: Parser {
    fn new(parser: impl Parser + 'static) -> Self;
}

pub struct AllCombinator {
    parser: Box<dyn Parser>,
}

impl Combinator for AllCombinator {
    fn new(parser: impl Parser + 'static) -> Self {
        return AllCombinator { parser: Box::new(parser) };
    }
}

impl Parser for AllCombinator {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let mut count = 0;
        let mut remainder = input.clone();

        while let Some((whitespace, _remainder)) = self.parser.parse(remainder) {
            remainder = _remainder;
            count += whitespace.chars().map(|c| c.len_utf8()).sum::<usize>();
        }

        if count == 0 {
            return None;
        }

        return Some((input[..count].into(), input[count..].into()));
    }
}

impl AllCombinator {
    pub fn all(self, parser: impl Parser + 'static) -> Self {
        return AllCombinator { parser: Box::new(parser) };
    }
}


pub struct AnyCombinator {
    parser: Box<dyn Parser>
}

impl Combinator for AnyCombinator {
    fn new(parser: impl Parser + 'static) -> Self {
        return AnyCombinator { parser: Box::new(parser) };
    }
}

impl Parser for AnyCombinator {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let mut count = 0;
        let mut remainder = input.clone();

        while let Some((whitespace, _remainder)) = self.parser.parse(remainder) {
            remainder = _remainder;
            count += whitespace.chars().map(|c| c.len_utf8()).sum::<usize>();
        }

        return Some((input[..count].into(), input[count..].into()));
    }
}

impl AnyCombinator {
    pub fn any(self, parser: impl Parser + 'static) -> Self {
        return AnyCombinator { parser: Box::new(parser) };
    }
}


pub struct OrCombinator {
    parsers: Vec<Box<dyn Parser>>,
}

impl Combinator for OrCombinator {
    fn new(parser: impl Parser + 'static) -> Self {
        return OrCombinator { parsers: vec![Box::new(parser)] };
    }
}

impl Parser for OrCombinator {
    fn parse(&self, input: String) -> Option<(String, String)> {
        for parser in &self.parsers {
            if let Some((matched, remainder)) = parser.parse(input.clone()) {
                return Some((matched, remainder));
            }
        }

        return None;
    }
}

impl OrCombinator {
    pub fn or(mut self, parser: impl Parser + 'static) -> Self {
        self.parsers.push(Box::new(parser));
        return self;
    }
}


pub struct ThenCombinator {
    parser: Box<dyn Parser>,
    then: Option<Box<dyn Parser>>,
}

impl Combinator for ThenCombinator {
    fn new(parser: impl Parser + 'static) -> Self {
        return ThenCombinator { parser: Box::new(parser), then: None };
    }
}

impl Parser for ThenCombinator {
    fn parse(&self, input: String) -> Option<(String, String)> {
        return self.parser.parse(input).and_then(|(matched, remainder)| {
            self.then.as_ref()?
                .parse(remainder)
                .map(|(then_matched, remainder)| (matched + &then_matched, remainder))
        });
    }
}

impl ThenCombinator {
    pub fn then(mut self, parser: impl Parser + 'static) -> Self {
        self.then = Some(Box::new(parser));
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
        let parser = AllCombinator::new(WhitespaceParser);

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(parser.parse("a".into()), None);
        assert_eq!(parser.parse("  ".into()), Some(("  ".into(), "".into())));
        assert_eq!(parser.parse(" a ".into()), Some((" ".into(), "a ".into())));
        assert_eq!(parser.parse("a ".into()), None);
        assert_eq!(parser.parse(" \t           asdf".into()), Some((" \t           ".into(), "asdf".into())));
    }

    #[test]
    fn test_some_combinator() {
        let parser = OrCombinator::new(WhitespaceParser)
            .or(LetterParser)
            .or(SpecialCharParser);

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(parser.parse("  ".into()), Some((" ".into(), " ".into())));
        assert_eq!(parser.parse("1 ".into()), None);

        let parser = parser.or(DigitParser);

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(parser.parse("1".into()), Some(("1".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
    }

    #[test]
    fn test_then_combinator() {
        let parser = ThenCombinator::new(DigitParser).then(LetterParser);

        assert_eq!(parser.parse("1".into()), None);
        assert_eq!(parser.parse("1a".into()), Some(("1a".into(), "".into())));
        assert_eq!(parser.parse("a1".into()), None);
        assert_eq!(parser.parse("11".into()), None);
        assert_eq!(parser.parse(" a".into()), None);
    }

    #[test]
    fn test_combining_combinators() {
        let parser = WhitespaceParser.all().then(
            LetterParser.or(DigitParser)
                .or(SpecialCharParser)
            );

        assert_eq!(parser.parse(" ".into()), None);
        assert_eq!(parser.parse("a1".into()), None);
        assert_eq!(parser.parse("  ".into()), None);
        assert_eq!(parser.parse(" 1a".into()), Some((" 1".into(), "a".into())));
        assert_eq!(parser.parse(" <1".into()), Some((" <".into(), "1".into())));
    }
}
