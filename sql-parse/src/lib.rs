mod parsers;

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnSelector {
    Asterisk,
    Name(String)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Token {
    Select,
    Column(ColumnSelector),
    From,
    Table(String),
    Semicolon,
}

pub trait Parser {
    fn parse<'a>(&'a self, input: &'a str) -> Option<(&str, &str)>;
}

pub trait Combinator {
    fn new(parser: impl Parser + 'static) -> Self;

    fn parse<'a>(&'a self, input: &'a str) -> Option<(&str, &str)>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_simple_select() {
    }
}
