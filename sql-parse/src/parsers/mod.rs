use core::fmt::Debug;
use dyn_clone::DynClone;

mod combinators;
mod grammar;
mod chaining;

pub use combinators::{All, Any, Or, Then};
pub use chaining::Chain;
pub use grammar::Keyword;

use super::primitives;

trait Parser: Debug + DynClone {
    fn parse(&self, input: String) -> Option<(Vec<Token>, String)>;
}

dyn_clone::clone_trait_object!(Parser);

#[derive(Debug)]
pub enum Token {
    Select,
    From,
    Identifier(String),
    Asterisk,
    Comma,
    Semicolon,
    Whitespace,
}
