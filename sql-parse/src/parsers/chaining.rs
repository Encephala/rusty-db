//! Provides a blanket implementation for the `ThenOr` trait,
//! which allows for chaining parsers and combinators together using the `then` and `or` methods.

use super::combinators::{Combinator, AllCombinator, AnyCombinator, ThenCombinator, OrCombinator};
use super::primitives::Parser;

pub trait CombinatorChain {
    fn all(self) -> AllCombinator
    where Self: Parser + Sized + 'static {
        return AllCombinator::new(self);
    }

    fn any(self) -> AnyCombinator
    where Self: Parser + Sized + 'static {
        return AnyCombinator::new(self);
    }

    fn or(self, parser: impl Parser + 'static) -> OrCombinator
    where Self: Parser + Sized + 'static {
        return OrCombinator::new(self).or(parser);
    }

    fn then(self, parser: impl Parser + 'static) -> ThenCombinator
    where Self: Parser + Sized + 'static {
        return ThenCombinator::new(self).then(parser);
    }
}
