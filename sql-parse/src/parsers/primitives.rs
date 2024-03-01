pub trait Parser {
    fn parse(&self, input: String) -> Option<(String, String)>;
}

fn parse_if(input: String, predicate: fn(char) -> bool) -> Option<(String, String)> {
    let condition = input.chars().next().map(predicate)?;

    if condition {
        let index_second_char = input.chars().next().map(|c| c.len_utf8())?;

        return Some((input[..index_second_char].into(), input[index_second_char..].into()));
    }

    return None;
}

pub struct WhitespaceParser;
impl Parser for WhitespaceParser {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_whitespace())
    }
}


pub struct LetterParser;
impl Parser for LetterParser {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_alphabetic())
    }
}


pub struct DigitParser;
impl Parser for DigitParser {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| c.is_ascii_digit())
    }
}


const SPECIAL_CHARS: [char; 11] = [
    ' ',
    '"',
    '\'',
    '(',
    ')',
    '*',
    ',',
    '.',
    '<',
    '>',
    '=',
];

pub struct SpecialCharParser;
impl Parser for SpecialCharParser {
    fn parse(&self, input: String) -> Option<(String, String)> {
        parse_if(input, |c| SPECIAL_CHARS.contains(&c))
    }
}


#[cfg(test)]
mod tests {
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

    #[test]
    fn test_letter_parser() {
        let parser = LetterParser;

        assert_eq!(parser.parse("a".into()), Some(("a".into(), "".into())));
        assert_eq!(parser.parse("A".into()), Some(("A".into(), "".into())));
        assert_eq!(parser.parse("1".into()), None);
        assert_eq!(parser.parse(" ".into()), None);
    }
}
