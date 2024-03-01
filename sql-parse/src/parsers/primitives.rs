use super::Parser;

pub struct WhitespaceParser;
impl Parser for WhitespaceParser {
    fn parse(&self, input: String) -> Option<(String, String)> {
        let is_whitespace = input.chars().next().map(|c| c.is_whitespace())?;

        if is_whitespace {
            let index_second_char = input.chars().next().map(|c| c.len_utf8())?;

            return Some((input[..index_second_char].into(), input[index_second_char..].into()));
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

        assert_eq!(parser.parse(" ".into()), Some((" ".into(), "".into())));
        assert_eq!(parser.parse(" a".into()), Some((" ".into(), "a".into())));
        assert_eq!(parser.parse("a".into()), None);
    }

    #[test]
    fn test_whitespace_parser_all_unicode_whitespace() {
        let parser = WhitespaceParser;

        assert_eq!(parser.parse(" \t           asdf".into()), Some((" ".into(), "\t           asdf".into())));
        assert_eq!(parser.parse("\t           asdf".into()), Some(("\t".into(), "           asdf".into())));
        assert_eq!(parser.parse("           asdf".into()), Some((" ".into(), "          asdf".into())));
        assert_eq!(parser.parse("          asdf".into()), Some((" ".into(), "         asdf".into())));
        assert_eq!(parser.parse("         asdf".into()), Some((" ".into(), "        asdf".into())));
        assert_eq!(parser.parse("        asdf".into()), Some((" ".into(), "       asdf".into())));
        assert_eq!(parser.parse("       asdf".into()), Some((" ".into(), "      asdf".into())));
        assert_eq!(parser.parse("      asdf".into()), Some((" ".into(), "     asdf".into())));
        assert_eq!(parser.parse("     asdf".into()), Some((" ".into(), "    asdf".into())));
        assert_eq!(parser.parse("    asdf".into()), Some((" ".into(), "   asdf".into())));
        assert_eq!(parser.parse("   asdf".into()), Some((" ".into(), "  asdf".into())));
        assert_eq!(parser.parse("  asdf".into()), Some((" ".into(), " asdf".into())));
        assert_eq!(parser.parse(" asdf".into()), Some((" ".into(), "asdf".into())));
        assert_eq!(parser.parse("asdf".into()), None);
    }
}
