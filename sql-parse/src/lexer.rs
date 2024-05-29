use std::str::Chars;

#[derive(Debug, PartialEq)]
pub enum Token {
    Select,
    From,
    Where,
    Ident(String),
    Int(usize),
    Decimal(usize, usize),

    Asterisk,
    Comma,
    Semicolon,

    Equals,
    NotEquals,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,

    Plus,
    Minus,
    // Multiplication and <all columns> share the same token
    // Times,
    Slash,

    Invalid,
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
    pub fn new(input: &'a str) -> Self {
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

    pub fn lex(&mut self) -> Vec<Token> {
        let mut result = vec![];

        while self.current_char.is_some() {
            self.skip_whitespace();

            result.push(self.next_token());
        }

        return result;
    }

    fn next_token(&mut self) -> Token {
        use Token::*;

        // Can unwrap because self.lex() checks for is_some
        let token = match self.current_char.unwrap() {
            ',' => Comma,
            ';' => Semicolon,
            '*' => Asterisk,
            '=' => Equals,
            '<' => {
                match self.next_char {
                    Some('>') => {
                        self.advance();

                        NotEquals
                    },
                    Some('=') => {
                        self.advance();

                        LessThanEqual
                    }
                    _ => LessThan,
                }
            }
            '>' => {
                match self.next_char {
                    Some('=') => {
                        self.advance();

                        GreaterThanEqual
                    }
                    _ => GreaterThan,
                }
            }
            '+' => Plus,
            '-' => Minus,
            '/' => Slash,

            c if c.is_alphabetic() => return self.read_identifier(),
            c if c.is_numeric() => return self.read_number(),

            _ => Invalid,
        };

        self.advance();

        return token;
    }

    fn read_identifier(&mut self) -> Token {
        let mut result = String::new();

        while let Some(current_char) = self.current_char {
            if !current_char.is_alphanumeric() &&
                current_char != '_'
            {
                break;
            }

            result.push(current_char);
            self.advance();
        };

        return Token::from(result);
    }

    fn read_number(&mut self) -> Token {
        let mut result = String::new();

        while let Some(current_char) = self.current_char {
            if !current_char.is_numeric() &&
                current_char != '.'
            {
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

                Token::Decimal(whole.parse().unwrap(), fractional.parse().unwrap())
            },
            _ => {
                panic!("Invalid decimal number found, had {number_of_dots} decimal points");
            }
        }
     }
}

#[cfg(test)]
mod tests {
    use super::{Token::*, Lexer};

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
    fn simple_select_statement() {
        let input = "SELECT blabla FROM asdf WHERE your_mom;";

        let mut lexer = Lexer::new(input);

        let result = lexer.lex();

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
            ]
        );
    }

    #[test]
    fn all_symbols() {
        let mut lexer = Lexer::new(
            ", ; * = < <= > >= + - /"
        );

        assert_eq!(
            lexer.lex(),
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
            ]
        );
    }

    #[test]
    fn symbols_in_query() {
        let mut lexer = Lexer::new(
            "SELECT * FROM bla WHERE asdf <> 5;",
        );

        // TODO
        assert_eq!(
            lexer.lex(),
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
            ]
        )
    }

    #[test]
    fn identifier_list() {
        let result = Lexer::new(
            "SELECT a,b, asdf FROM c;"
        ).lex();

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
            ]
        )
    }
}
