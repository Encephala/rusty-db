use super::{Parser, Combinator};

struct MultipleCombinator {
    parser: Box<dyn Parser + 'static>,
}

impl Combinator for MultipleCombinator {
    fn new(parser: impl Parser + 'static) -> Self {
        MultipleCombinator { parser: Box::new(parser) }
    }

    fn parse<'a>(&'a self, input: &'a str) -> Option<(&str, &str)> {
        let mut count = 0;
        let mut remainder = input;

        while let Some((whitespace, _remainder)) = self.parser.parse(remainder) {
            remainder = _remainder;
            count += whitespace.chars().map(|c| c.len_utf8()).sum::<usize>();
        }

        if count == 0 {
            return None;
        }

        return Some((&input[..count], &input[count..]));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::primitives::WhitespaceParser;

    #[test]
    fn test_multiple_combinator() {
        let parser = MultipleCombinator::new(WhitespaceParser);

        assert_eq!(parser.parse(" "), Some((" ", "")));
        assert_eq!(parser.parse(" a"), Some((" ", "a")));
        assert_eq!(parser.parse("a"), None);
        assert_eq!(parser.parse("  "), Some(("  ", "")));
        assert_eq!(parser.parse(" a "), Some((" ", "a ")));
        assert_eq!(parser.parse("a "), None);
        assert_eq!(parser.parse(" \t           asdf"), Some((" \t           ", "asdf")));
    }
}
