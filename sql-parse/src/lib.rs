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
    fn parse(&self, input: String) -> Option<(String, String)>;
}

pub trait Combinator {
    fn new(parser: impl Parser + 'static) -> Self;

    fn parse(&self, input: String) -> Option<(String, String)>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_simple_select() {
    }
}
