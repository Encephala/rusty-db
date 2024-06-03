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
pub struct Then {
    parser: Box<dyn ExpressionParser>,
    next_parser: Option<Box<dyn ExpressionParser>>,
}

impl Then {
    fn _new(parser: impl ExpressionParser + 'static) -> Self {
        return Then {
            parser: Box::new(parser),
            next_parser: None,
        }
    }

    fn _then(mut self, new_next_parser: impl ExpressionParser + 'static) -> Self {
        // self.next_parser is None, can insert immediately
        if self.next_parser.is_none() {
            self.next_parser = Some(Box::new(new_next_parser));

            return self;
        }

        // self.next_parser is not None,
        // have to first parse self.parser and self.next_parser, then parse new_next_parser
        self.next_parser = Some(Box::new(Then {
            parser: self.next_parser.unwrap(),
            next_parser: Some(Box::new(new_next_parser)),
        }));

        return self;
    }
}

impl ExpressionParser for Then {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        let first = self.parser.parse(input)?;
        let next = self.next_parser.as_ref()?.parse(input)?;

        // TODO: this should be recursive
        let result = match (first, next) {
            (Expression::Array(mut array), next) => {
                array.push(next);

                Expression::Array(array)
            },
            (first, Expression::Array(ref mut array)) => {
                let mut result = vec![first];

                result.append(array);

                Expression::Array(result)
            },
            (first, next) => Expression::Array(vec![first, next]),
        };

        return Some(result);
    }
}


pub trait Chain {
    fn or(self, parser: impl ExpressionParser + 'static) -> Or
    where Self: ExpressionParser + Sized + 'static {
        return Or::new(self).or(parser);
    }

    fn then(self, parser: impl ExpressionParser + 'static) -> Then
    where Self: ExpressionParser + Sized + 'static {
        return Then::_new(self)._then(parser);
    }
}

impl<T: ExpressionParser> Chain for T {}

#[cfg(test)]
mod tests {
    use super::Chain;
    use super::super::expressions::{ExpressionParser, Expression as E, Identifier, Number};
    use crate::lexer::Lexer;

    #[test]
    fn or_basic() {

    }

    #[test]
    fn then_basic() {
        let inputs = [
            (
                Identifier.then(Number)._then(Identifier),
                "asdf 123 bla",
                Some(E::Array(vec![
                    E::Ident("asdf".into()),
                    E::Int(123),
                    E::Ident("bla".into()),
                ])),
            ),
        ];

        inputs.iter().for_each(|test_case| {
            let result = test_case.0.parse(
                &mut Lexer::lex(test_case.1).as_slice()
            );

            assert_eq!(result, test_case.2);
        })
    }
}
