use super::Parser;

pub trait Combinator<P: Parser> {
    fn parse(input: &str, parser: P) -> Option<(&str, &str)>;
}

struct MultipleCombinator;
impl<P: Parser> Combinator<P> for MultipleCombinator {
    fn parse(input: &str, parser: P) -> Option<(&str, &str)> {
        let mut count = 0;
        let mut remainder = input;

        while let Some((_, _remainder)) = parser.parse(remainder) {
            remainder = _remainder;
            count += 1;
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
        assert_eq!(MultipleCombinator::parse(" ", WhitespaceParser), Some((" ", "")));
        assert_eq!(MultipleCombinator::parse(" a", WhitespaceParser), Some((" ", "a")));
        assert_eq!(MultipleCombinator::parse("a", WhitespaceParser), None);
        assert_eq!(MultipleCombinator::parse("  ", WhitespaceParser), Some(("  ", "")));
        assert_eq!(MultipleCombinator::parse(" a ", WhitespaceParser), Some((" ", "a ")));
        assert_eq!(MultipleCombinator::parse("a ", WhitespaceParser), None);
    }
}
