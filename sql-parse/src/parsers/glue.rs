//! Provides a blanket implementation for the `ThenOr` trait,
//! which allows for chaining parsers and combinators together using the `then` and `or` methods.

use super::combinators::{Combinator, ThenCombinator, SomeCombinator};
use super::primitives::Parser;

pub trait ThenOr {
    fn then(self, parser: impl Parser + 'static) -> ThenCombinator
    where Self: Parser + Sized + 'static {
        return ThenCombinator::new(self).then(parser);
    }

    fn or(self, parser: impl Parser + 'static) -> SomeCombinator
    where Self: Parser + Sized + 'static {
        return SomeCombinator::new(self).or(parser);
    }
}
