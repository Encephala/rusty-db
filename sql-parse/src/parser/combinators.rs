use crate::lexer::Token;
use super::expressions::{ExpressionParser, Expression};

pub struct Or {
    parsers: Vec<Box<dyn ExpressionParser>>
}

impl Or {
    fn new(parser: impl ExpressionParser + 'static) -> Self {
        return Or { parsers: vec![Box::new(parser)]};
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

pub trait Chain {
    fn or(self, parser: impl ExpressionParser + 'static) -> Or
    where Self: ExpressionParser + Sized + 'static {
        return Or::new(self).or(parser);
    }
}

impl<T: ExpressionParser> Chain for T {}
