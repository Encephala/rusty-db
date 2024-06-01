use crate::lexer::Token;
use super::utils::check_and_skip;
use super::expressions::{Expression, IdentifierParser, Parser, WhereParser};

#[derive(Debug, PartialEq)]
pub enum Statement {
    Select {
        column: Expression,
        table: Expression,
        where_clause: Option<Expression>,
    },
}


struct SelectStatement;

impl SelectStatement {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Select)?;

        let column = IdentifierParser.parse(input)?;

        check_and_skip(input, Token::From)?;

        let table = IdentifierParser.parse(input)?;

        let where_clause = WhereParser.parse(input);

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Select {
            column,
            table,
            where_clause,
        });
    }
}


#[cfg(test)]
mod tests {
    use crate::lexer::Lexer;

    use super::{
        Parser,
        Statement as S,
        SelectStatement,
        Expression as E,
    };

    #[test]
    fn basic_select() {
        let input = "SELECT bla from asdf;";

        let tokens = Lexer::new(input).lex();

        let result = SelectStatement.parse(tokens.as_slice());

        assert_eq!(
            result,
            Some(S::Select {
                column: E::Ident("bla".into()),
                table: E::Ident("asdf".into()),
                where_clause: None,
            }));
    }
}
