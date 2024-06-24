use std::str::Chars;

#[derive(Debug, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Create,
    Database,
    Table,
    Update,
    Set,
    Delete,
    Drop,

    // Types
    TypeInt,
    TypeDecimal,
    TypeText,
    TypeBool,

    // Literals
    Ident(String),
    Int(usize),
    Decimal(usize, usize),
    Str(String),
    Bool(bool),

    // Symbols
    Asterisk,
    Comma,
    Semicolon,
    LParenthesis,
    RParenthesis,

    // Infix operators
    Equals,
    NotEquals,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,

    // Arithmetic operators
    Plus,
    Minus,
    // Multiplication and <all columns> share the same token
    // Times,
    Slash,

    Eof,

    Invalid(String),
}

impl From<String> for Token {
    /// Matches the given word for keywords and returns the corresponding token.
    /// If no matching keyword is found, the given word is considered an identifier.
    fn from(value: String) -> Self {
        use Token::*;

        return match value.to_uppercase().as_ref() {
            "SELECT" => Select,
            "FROM" => From,
            "WHERE" => Where,
            "INSERT" => Insert,
            "INTO" => Into,
            "CREATE" => Create,
            "VALUES" => Values,
            "DATABASE" => Database,
            "TABLE" => Table,
            "UPDATE" => Update,
            "SET" => Set,
            "DELETE" => Delete,
            "DROP" => Drop,
            "INT" => TypeInt,
            "INTEGER" => TypeInt,
            "DECIMAL" => TypeDecimal,
            "TEXT" => TypeText,
            "BOOL" => TypeBool,
            "BOOLEAN" => TypeBool,
            // Hijacking from_identifier to parse boolean literals
            "TRUE" => Bool(true),
            "FALSE" => Bool(false),
            _ => Ident(value),
        };
    }
}

pub struct Lexer<'a> {
    input: std::iter::Peekable<Chars<'a>>,
    current_char: Option<char>,
    next_char: Option<char>,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        let mut result = Self {
            input: input.chars().peekable(),
            current_char: None,
            next_char: None,
        };

        // Prepare first character
        result.advance();

        return result;
    }

    fn advance(&mut self) {
        self.current_char = self.input.next();

        self.next_char = self.input.peek().cloned();
    }

    fn skip_whitespace(&mut self) {
        while let Some(true) = self.current_char.map(char::is_whitespace) {
            self.advance();
        }
    }

    pub fn lex(input: &'a str) -> Vec<Token> {
        let mut lexer = Lexer::new(input);

        let mut tokens = vec![];

        // Skip leading whitespace
        lexer.skip_whitespace();

        while lexer.current_char.is_some() {
            tokens.push(lexer.next_token());

            // Skip intermediate/trailing whitespace
            lexer.skip_whitespace();
        }

        tokens.push(Token::Eof);

        return tokens;
    }

    fn next_token(&mut self) -> Token {
        use Token::*;

        // Can unwrap because lex() checks is_some
        let token = match self.current_char.unwrap() {
            ',' => Comma,
            ';' => Semicolon,
            '*' => Asterisk,
            '(' => LParenthesis,
            ')' => RParenthesis,
            '=' => Equals,
            '\'' => {
                self.advance();

                let mut result = String::new();

                while let Some(character) = self.current_char {
                    if character == '\'' {
                        break;
                    }

                    result.push(character);

                    self.advance();
                }

                Str(result)
            }
            '<' => match self.next_char {
                Some('>') => {
                    self.advance();

                    NotEquals
                }
                Some('=') => {
                    self.advance();

                    LessThanEqual
                }
                _ => LessThan,
            },
            '>' => match self.next_char {
                Some('=') => {
                    self.advance();

                    GreaterThanEqual
                }
                _ => GreaterThan,
            },
            '+' => Plus,
            '-' => Minus,
            '/' => Slash,

            c if c.is_alphabetic() => return self.read_identifier(),
            c if c.is_numeric() => return self.read_number(),

            other => Invalid(format!("Unknown character '{other}'")),
        };

        self.advance();

        return token;
    }

    fn read_identifier(&mut self) -> Token {
        // Note: identifier has to start with letter but may contain numbers
        let mut result = String::new();

        while let Some(current_char) = self.current_char {
            if !current_char.is_alphanumeric() && current_char != '_' {
                break;
            }

            result.push(current_char);
            self.advance();
        }

        return Token::from(result);
    }

    fn read_number(&mut self) -> Token {
        let mut result = String::new();

        while let Some(current_char) = self.current_char {
            if !current_char.is_numeric() && current_char != '.' {
                break;
            }

            result.push(current_char);
            self.advance();
        }

        let number_of_dots = result.chars().filter(|char| char == &'.').count();

        return match number_of_dots {
            0 => Token::Int(result.parse().unwrap()),
            1 => {
                let (whole, fractional) = result.split_once('.').unwrap();

                let whole = whole.parse().unwrap();
                let fractional = fractional.parse();

                if let Ok(fractional) = fractional {
                    Token::Decimal(whole, fractional)
                } else {
                    Token::Invalid(format!("No number found after decimal dot in {result}"))
                }
            }
            _ => Token::Invalid(format!(
                "Found {number_of_dots} decimal separators in number '{result}'"
            )),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::{Lexer, Token::*};

    #[test]
    fn lexer_advance() {
        let input = "asdf";

        let mut lexer = Lexer::new(input);

        assert_eq!(lexer.current_char, Some('a'));
        assert_eq!(lexer.next_char, Some('s'));

        for _ in 0..input.len() - 1 {
            lexer.advance();
        }

        assert_eq!(lexer.current_char, Some('f'));
        assert_eq!(lexer.next_char, None);

        lexer.advance();

        assert_eq!(lexer.current_char, None);
        assert_eq!(lexer.next_char, None);
    }

    #[test]
    fn keywords() {
        let input = " select from table bool boolean int integer text ";

        let result = Lexer::lex(input);

        assert_eq!(
            result,
            vec![Select, From, Table, TypeBool, TypeBool, TypeInt, TypeInt, TypeText, Eof,],
        )
    }

    #[test]
    fn handles_leading_and_trailing_whitespace() {
        let input = " select ";

        let result = Lexer::lex(input);

        assert_eq!(result, vec![Select, Eof])
    }

    #[test]
    fn simple_select_statement() {
        let input = "SELECT blabla FROM asdf WHERE your_mom;";

        let result = Lexer::lex(input);

        assert_eq!(
            result,
            vec![
                Select,
                Ident("blabla".into()),
                From,
                Ident("asdf".into()),
                Where,
                Ident("your_mom".into()),
                Semicolon,
                Eof,
            ]
        );
    }

    #[test]
    fn identifier_cant_start_with_number() {
        let input = "SELECT 1abc FROM asdf";

        let result = Lexer::lex(input);

        assert_eq!(
            result,
            vec![
                Select,
                Int(1),
                Ident("abc".into()),
                From,
                Ident("asdf".into()),
                Eof,
            ]
        );
    }

    #[test]
    fn all_symbols() {
        let result = Lexer::lex(", ; * = < <= > >= + - /");

        assert_eq!(
            result,
            vec![
                Comma,
                Semicolon,
                Asterisk,
                Equals,
                LessThan,
                LessThanEqual,
                GreaterThan,
                GreaterThanEqual,
                Plus,
                Minus,
                Slash,
                Eof,
            ]
        );
    }

    #[test]
    fn symbols_in_query() {
        let result = Lexer::lex("SELECT * FROM bla WHERE asdf <> 5;");

        assert_eq!(
            result,
            vec![
                Select,
                Asterisk,
                From,
                Ident("bla".into()),
                Where,
                Ident("asdf".into()),
                NotEquals,
                Int(5),
                Semicolon,
                Eof,
            ]
        )
    }

    #[test]
    fn identifier_list() {
        let result = Lexer::lex("SELECT a,b, asdf FROM c;");

        assert_eq!(
            result,
            vec![
                Select,
                Ident("a".into()),
                Comma,
                Ident("b".into()),
                Comma,
                Ident("asdf".into()),
                From,
                Ident("c".into()),
                Semicolon,
                Eof,
            ]
        )
    }

    #[test]
    fn bool() {
        let result = Lexer::lex("true false true true false though");

        assert_eq!(
            result,
            vec![
                Bool(true),
                Bool(false),
                Bool(true),
                Bool(true),
                Bool(false),
                Ident("though".into()),
                Eof,
            ]
        )
    }

    #[test]
    fn string() {
        let result = Lexer::lex("'asdfghjkl';");

        assert_eq!(result, vec![Str("asdfghjkl".into()), Semicolon, Eof,]);
    }

    #[test]
    fn handle_invalid_token() {
        assert_eq!(
            Lexer::lex("&"),
            vec![Invalid("Unknown character '&'".into()), Eof,]
        );

        assert_eq!(
            Lexer::lex("1 1.2 1.2.3"),
            vec![
                Int(1),
                Decimal(1, 2),
                Invalid("Found 2 decimal separators in number '1.2.3'".into()),
                Eof,
            ]
        );
    }
}
