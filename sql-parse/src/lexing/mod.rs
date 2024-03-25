mod tokens;
mod primitives;
mod combinators;
mod chaining;
mod grammar;

pub use primitives::{Tokeniser, Whitespace, Digit, Letter, Literal, Empty};
pub use combinators::{All, Any, Or, Then};
pub use chaining::Chain;
pub use tokens::Token;

pub trait Lexer {
    fn tokenise(&self, input: String) -> Vec<Token>;
}

struct KeywordLexer;

impl Lexer for KeywordLexer {
    fn tokenise(&self, input: String) -> Vec<Token> {
        let mut result = vec![];

        if let Some((matched, input)) = Letter.then(Letter.or(Digit).all()).consume(input) {
            match matched.as_str() {
                "SELECT" => result.push(Token::Select),
                "FROM" => result.push(Token::From),
                "CREATE" => result.push(Token::Create),
                "TABLE" => result.push(Token::Table),
                "DROP" => result.push(Token::Drop),
                "INSERT" => result.push(Token::Insert),
                "INTO" => result.push(Token::Into),
                "VALUES" => result.push(Token::Values),
                _ => result.push(Token::Word(matched)),
            }
        }

        return result;
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}
