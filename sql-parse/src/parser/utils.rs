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
