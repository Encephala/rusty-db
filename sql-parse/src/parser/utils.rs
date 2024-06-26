use crate::lexer::Token;

/// Checks whether the first token in put is `equals`.
///
/// If not, returns [`None`].
/// If so, advances input by one token and returns [`Some`].
pub fn check_and_skip(input: &mut &[Token], equals: Token) -> Option<()> {
    if input.first() != Some(&equals) {
        return None;
    }

    *input = &input[1..];

    return Some(());
}

#[cfg(test)]
mod tests {
    use super::check_and_skip;
    use crate::lexer::{Lexer, Token};

    #[test]
    fn check_and_skip_basic() {
        let input = Lexer::lex("DROP TABLE bla;");
        let input = &mut input.as_slice();

        assert_ne!(check_and_skip(input, Token::Drop), None);
        assert_ne!(check_and_skip(input, Token::Table), None);
        assert_ne!(check_and_skip(input, Token::Ident("bla".into())), None);
        assert_ne!(check_and_skip(input, Token::Semicolon), None);
    }
}
