use crate::lexer::Token;


/// Checks whether the first token in put is `equals`.
///
/// If not, returns [`None`].
/// If so, advances input by one token and returns [`Some`].
pub fn check_and_skip(input: &mut &[Token], equals: Token) -> Option<()> {
    if input.get(0) != Some(&equals) {
        return None;
    }

    *input = &mut &input[1..];

    return Some(());
}

#[cfg(test)]
mod tests {
    use crate::lexer::{Lexer, Token};
    use super::check_and_skip;

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
