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
    fn parse(self, input: &str) -> Option<(Token, &str)>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let input = "SELECT * FROM my_table;";
    }

    #[test]
    fn test_handle_multiple_whitespace() {
        let input = "SELECT \t           ​‌‍* FROM my_table;";
    }
}
