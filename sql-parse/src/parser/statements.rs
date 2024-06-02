use super::expressions::{Expression, Parser, Identifier, WhereParser};
use super::utils::check_and_skip;
use crate::lexer::Token;

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

        let column = Identifier.parse(input)?;

        check_and_skip(input, Token::From)?;

        let table = Identifier.parse(input)?;

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
    use super::super::expressions::InfixOperator;
    use super::{Expression as E, SelectStatement, Statement as S};


    #[test]
    fn select_basic() {
        let input = "SELECT bla from asdf;";

        let tokens = Lexer::new(input).lex();

        let result = SelectStatement.parse(tokens.as_slice());

        assert_eq!(
            result,
            Some(S::Select {
                column: E::Ident("bla".into()),
                table: E::Ident("asdf".into()),
                where_clause: None,
            })
        );
    }

    #[test]
    fn select_with_where() {
        let input = "SELECT bla FROM asdf WHERE a > b;";

        let result = SelectStatement.parse(Lexer::new(input).lex().as_slice());

        assert_eq!(
            result,
            Some(S::Select {
                column: E::Ident("bla".into()),
                table: E::Ident("asdf".into()),
                where_clause: Some(E::Where {
                    left: E::Ident("a".into()).into(),
                    operator: InfixOperator::GreaterThan,
                    right: E::Ident("b".into()).into(),
                })
            })
        )
    }
}
