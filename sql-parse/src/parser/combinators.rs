//! Combinators for ExpressionParsers.

use crate::lexer::Token;
use super::expressions::{ExpressionParser, Expression};

#[derive(Debug)]
pub struct Or {
    parsers: Vec<Box<dyn ExpressionParser>>
}

impl Or {
    fn new(parser: impl ExpressionParser + 'static) -> Self {
        return Or {
            parsers: vec![Box::new(parser)],
        };
    }

    pub fn or(mut self, parser: impl ExpressionParser + 'static) -> Self {
        self.parsers.push(Box::new(parser));
        return self;
    }
}

impl ExpressionParser for Or {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        for parser in &self.parsers {
            if let Some(result) = parser.parse(input) {
                return Some(result);
            }
        }

        return None;
    }
}



#[derive(Debug)]
pub struct Multiple {
    parser: Box<dyn ExpressionParser>
}
impl Multiple {
    fn new(parser: impl ExpressionParser + 'static) -> Self {
        return Multiple {
            parser: Box::new(parser)
        };
    }
}

impl ExpressionParser for Multiple {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        let mut expressions = vec![];

        expressions.push(self.parser.parse(input)?);

        while Some(&Token::Comma) == input.get(0) {
            *input = &input[1..];

            // Allow trailing comma at end of array
            // Idk if this is a good idea to have, probably hard to get proper
            // error messages if parsing somewhere else fails
            if let Some(Token::RParenthesis) = input.get(0) {
                break;
            }

            expressions.push(self.parser.parse(input)?);
        }

        return Some(Expression::Array(expressions));
    }
}


pub trait Chain {
    fn or(self, parser: impl ExpressionParser + 'static) -> Or
    where Self: ExpressionParser + Sized + 'static {
        return Or::new(self).or(parser);
    }

    fn multiple(self) -> Multiple
    where Self: ExpressionParser + Sized + 'static {
        return Multiple::new(self);
    }
}

impl<T: ExpressionParser> Chain for T {}

#[cfg(test)]
mod tests {
    use super::Chain;
    use super::super::expressions::{ExpressionParser, Expression as E, Identifier, Str, Number, Array};
    use crate::lexer::Lexer;

    #[test]
    fn or_basic() {
        let inputs = [
            (
                Str.or(Number),
                "'a'",
                Some(E::Str('a'.into()))
            ),
            (
                Str.or(Number),
                "5",
                Some(E::Int(5))
            ),
            (
                Array.or(Identifier),
                "('asdf', 1)",
                Some(E::Array(vec![
                    E::Str("asdf".into()),
                    E::Int(1),
                ])),
            ),
            (
                Array.or(Identifier),
                "hey",
                Some(E::Ident("hey".into())),
            ),
        ];

        inputs.iter().for_each(|test_case| {
            let result = test_case.0.parse(
                &mut Lexer::lex(test_case.1).as_slice()
            );

            assert_eq!(result, test_case.2);
        });
    }

    #[test]
    fn list_basic() {
        let inputs: [(Box<dyn ExpressionParser>, _, _); 5] = [
            (
                Box::new(Identifier.multiple()),
                "asdf, jkl",
                E::Array(vec![
                    E::Ident("asdf".into()),
                    E::Ident("jkl".into()),
                ]),
            ),
            (
                Box::new(Identifier.or(Number).multiple()),
                "asdf, 1234, 1.2",
                E::Array(vec![
                    E::Ident("asdf".into()),
                    E::Int(1234),
                    E::Decimal(1, 2),
                ]),
            ),
            (
                Box::new(Array.multiple()),
                "('asdf', 1), ('jkl', 2)",
                E::Array(vec![
                    E::Array(vec![
                        E::Str("asdf".into()),
                        E::Int(1),
                    ]),
                    E::Array(vec![
                        E::Str("jkl".into()),
                        E::Int(2),
                    ]),
                ]),
            ),
            // For funsies
            (
                Box::new(Array.multiple().or(Number)),
                "('asdf', 1), ('jkl', 2)",
                E::Array(vec![
                    E::Array(vec![
                        E::Str("asdf".into()),
                        E::Int(1),
                    ]),
                    E::Array(vec![
                        E::Str("jkl".into()),
                        E::Int(2),
                    ]),
                ]),
            ),
            (
                Box::new(Array.multiple().or(Number)),
                "1.2",
                E::Decimal(1, 2),
            ),
        ];

        inputs.into_iter().for_each(|test_case| {
            let result = test_case.0.parse(
                &mut Lexer::lex(test_case.1).as_slice()
            );

            assert_eq!(result, Some(test_case.2));
        });
    }
}
