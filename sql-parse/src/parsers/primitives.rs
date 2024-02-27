use super::Parser;

pub struct WhitespaceParser;
impl Parser for WhitespaceParser {
    fn parse(input: & str) -> Option<(&str, &str)> {
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
        assert_eq!(WhitespaceParser::parse(" "), Some((" ", "")));
        assert_eq!(WhitespaceParser::parse(" a"), Some((" ", "a")));
        assert_eq!(WhitespaceParser::parse("a"), None);
    }

    #[test]
    fn test_whitespace_parser_all_unicode_whitespace() {
        assert_eq!(WhitespaceParser::parse(" \t           asdf"), Some((" ", "\t           asdf")));
        assert_eq!(WhitespaceParser::parse("\t           asdf"), Some(("\t", "           asdf")));
        assert_eq!(WhitespaceParser::parse("           asdf"), Some((" ", "          asdf")));
        assert_eq!(WhitespaceParser::parse("          asdf"), Some((" ", "         asdf")));
        assert_eq!(WhitespaceParser::parse("         asdf"), Some((" ", "        asdf")));
        assert_eq!(WhitespaceParser::parse("        asdf"), Some((" ", "       asdf")));
        assert_eq!(WhitespaceParser::parse("       asdf"), Some((" ", "      asdf")));
        assert_eq!(WhitespaceParser::parse("      asdf"), Some((" ", "     asdf")));
        assert_eq!(WhitespaceParser::parse("     asdf"), Some((" ", "    asdf")));
        assert_eq!(WhitespaceParser::parse("    asdf"), Some((" ", "   asdf")));
        assert_eq!(WhitespaceParser::parse("   asdf"), Some((" ", "  asdf")));
        assert_eq!(WhitespaceParser::parse("  asdf"), Some((" ", " asdf")));
        assert_eq!(WhitespaceParser::parse(" asdf"), Some((" ", "asdf")));
        assert_eq!(WhitespaceParser::parse("asdf"), None);
    }
}
