//! Defines the [`CombinatorChain`] trait, which is implemented by all parsers.
//!
//! This enables for chaining parsers and combinators together through the builder pattern using
//! - [`CombinatorChain::all`]: Create an [`AllCombinator`] from `self`.
//! - [`CombinatorChain::any`]: Create an [`AnyCombinator`] from `self`.
//! - [`CombinatorChain::or`]: Create an [`OrCombinator`] from `self` and the given parser.
//! - [`CombinatorChain::then`]: Create a [`ThenCombinator`] from `self` and the given parser.

use super::combinators::{Combinator, AllCombinator, AnyCombinator, ThenCombinator, OrCombinator};
use super::primitives::Parser;

/// Create combinator parsers through the builder pattern.
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
