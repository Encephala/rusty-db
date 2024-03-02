//! Defines the [`CombinatorChain`] trait, which is implemented by all parsers.
//!
//! This enables for chaining parsers and combinators together through the builder pattern using
//! - [`Chain::all`]: Create an [`All`] from `self`.
//! - [`Chain::any`]: Create an [`Any`] from `self`.
//! - [`Chain::or`]: Create an [`Or`] from `self` and the given parser.
//! - [`Chain::then`]: Create a [`Then`] from `self` and the given parser.

use super::combinators::{Combinator, All, Any, Then, Or};
use super::primitives::Parser;

/// Create combinator parsers through the builder pattern.
pub trait Chain {
    fn all(self) -> All
    where Self: Parser + Sized + 'static {
        return All::new(self);
    }

    fn any(self) -> Any
    where Self: Parser + Sized + 'static {
        return Any::new(self);
    }

    fn or(self, parser: impl Parser + 'static) -> Or
    where Self: Parser + Sized + 'static {
        return Or::new(self).or(parser);
    }

    fn then(self, parser: impl Parser + 'static) -> Then
    where Self: Parser + Sized + 'static {
        return Then::new(self).then(parser);
    }
}
