#[derive(Debug, PartialEq, Eq)]
enum ColumnSelector {
    Asterisk,
    Name(String)
}

#[derive(Debug, PartialEq, Eq)]
enum Token {
    Whitespace,
    Select,
    Column(ColumnSelector),
    Comma,
    From,
    Table(String),
    Semicolon,
}

trait Parser {
    fn parse(self, input: &str) -> Option<(Token, &str)>;
}

struct QueryParser;
impl QueryParser {
    fn parse(self, input: &str) -> Option<Vec<Token>> {
        let mut input = input;
        let mut tokens: Vec<Token> = Vec::new();

        if let Some((token, remainder)) = SelectParser.parse(input) {
            tokens.push(token);
            input = remainder;
        }

        if let Some((_, remainder)) = WhiteSpaceParser.parse(input) {
            input = remainder;
        }

        while let Some((token, remainder)) = ColumnParser.parse(input) {
            tokens.push(token);
            input = remainder;
        }

        if let Some((_, remainder)) = WhiteSpaceParser.parse(input) {
            input = remainder;
        }

        if let Some((token, remainder)) = FromParser.parse(input) {
            tokens.push(token);
            input = remainder;
        }

        if let Some((_, remainder)) = WhiteSpaceParser.parse(input) {
            input = remainder;
        }

        if let Some((token, remainder)) = TableParser.parse(input) {
            tokens.push(token);
            input = remainder;
        }

        if !input.is_empty() {
            return None;
        }

        return Some(tokens);
    }
}

struct WhiteSpaceParser;
impl Parser for WhiteSpaceParser {
    fn parse(self, input: &str) -> Option<(Token, &str)> {
        if let Some((_, remainder)) = input.split_once(|char: char| char.is_whitespace()) {
            return Some((Token::Whitespace, remainder));
        }

        None
    }
}

struct SelectParser;
impl Parser for SelectParser {
    fn parse(self, input: &str) -> Option<(Token, &str)> {
        if input.to_uppercase().strip_prefix("SELECT").is_some() {
            return Some((Token::Select, &input[6..]));
        }

        None
    }
}

struct ColumnParser;
impl Parser for ColumnParser {
    fn parse(self, input: &str) -> Option<(Token, &str)> {
        if let Some((column, remainder)) = input.split_once(|char: char| char == ',') {
            if column == "*" {
                return Some((Token::Column(ColumnSelector::Asterisk), remainder));
            }

            return Some((Token::Column(ColumnSelector::Name(column.into())), remainder));
        }

        if let Some((column, remainder)) = input.split_once(|char: char| char.is_whitespace()) {
            if column == "*" {
                return Some((Token::Column(ColumnSelector::Asterisk), remainder));
            }

            return Some((Token::Column(ColumnSelector::Name(column.into())), remainder));
        }

        return None;
    }
}

struct FromParser;
impl Parser for FromParser {
    fn parse(self, input: &str) -> Option<(Token, &str)> {
        if input.to_uppercase().strip_prefix("FROM").is_some() {
            return Some((Token::Select, &input[4..]));
        }

        None
    }
}

struct TableParser;
impl Parser for TableParser {
    fn parse(self, input: &str) -> Option<(Token, &str)> {
        if let Some((table, remainder)) = input.split_once(|char: char| char == ';' || char.is_whitespace()) {
            return Some((Token::Table(table.into()), remainder));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main() {
        let input = "SELECT * FROM my_table;";

        let tokens = QueryParser.parse(input);

        assert_eq!(tokens, Some(vec![
            Token::Select,
            Token::Column(ColumnSelector::Asterisk),
            Token::From,
            Token::Table("my_table".into()),
        ]))
    }
}
