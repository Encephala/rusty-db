use crate::lexer::Token;
use super::expressions::{Parser, Expression};

pub struct Or {
    parsers: Vec<Box<dyn Parser>>
}

impl Or {
    fn new(parser: impl Parser + 'static) -> Self {
        return Or { parsers: vec![Box::new(parser)]};
    }

    pub fn or(mut self, parser: impl Parser + 'static) -> Self {
        self.parsers.push(Box::new(parser));
        return self;
    }
}

impl Parser for Or {
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
    fn or(self, parser: impl Parser + 'static) -> Or
    where Self: Parser + Sized + 'static {
        return Or::new(self).or(parser);
    }
}

impl<T: Parser> Chain for T {}
