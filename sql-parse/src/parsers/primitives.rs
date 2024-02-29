use super::Parser;

pub struct WhitespaceParser;
impl Parser for WhitespaceParser {
    fn parse<'a>(&'a self, input: &'a str) -> Option<(&str, &str)> {
        let is_whitespace = input.chars().next().map(|c| c.is_whitespace())?;

        if is_whitespace {
            let index_second_char = input.chars().next().map(|c| c.len_utf8())?;

            return Some(input.split_at(index_second_char));
        }

        return None;
    }
}

#[cfg(test)]
mod asdf {
    use super::*;

    #[test]
    fn test_whitespace_parser() {
        let parser = WhitespaceParser;

        assert_eq!(parser.parse(" "), Some((" ", "")));
        assert_eq!(parser.parse(" a"), Some((" ", "a")));
        assert_eq!(parser.parse("a"), None);
    }

    #[test]
    fn test_whitespace_parser_all_unicode_whitespace() {
        let parser = WhitespaceParser;

        assert_eq!(parser.parse(" \t           asdf"), Some((" ", "\t           asdf")));
        assert_eq!(parser.parse("\t           asdf"), Some(("\t", "           asdf")));
        assert_eq!(parser.parse("           asdf"), Some((" ", "          asdf")));
        assert_eq!(parser.parse("          asdf"), Some((" ", "         asdf")));
        assert_eq!(parser.parse("         asdf"), Some((" ", "        asdf")));
        assert_eq!(parser.parse("        asdf"), Some((" ", "       asdf")));
        assert_eq!(parser.parse("       asdf"), Some((" ", "      asdf")));
        assert_eq!(parser.parse("      asdf"), Some((" ", "     asdf")));
        assert_eq!(parser.parse("     asdf"), Some((" ", "    asdf")));
        assert_eq!(parser.parse("    asdf"), Some((" ", "   asdf")));
        assert_eq!(parser.parse("   asdf"), Some((" ", "  asdf")));
        assert_eq!(parser.parse("  asdf"), Some((" ", " asdf")));
        assert_eq!(parser.parse(" asdf"), Some((" ", "asdf")));
        assert_eq!(parser.parse("asdf"), None);
    }
}
