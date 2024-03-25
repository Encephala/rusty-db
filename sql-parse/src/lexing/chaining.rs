//! Defines the [`CombinatorChain`] trait, and implements it for all types that are Tokeniser.
//!
//! This enables for chaining lexers and combinators together through the builder pattern using
//! - [`Chain::all`]: Create an [`All`] from `self`.
//! - [`Chain::any`]: Create an [`Any`] from `self`.
//! - [`Chain::or`]: Create an [`Or`] from `self` and the given lexer.
//! - [`Chain::then`]: Create a [`Then`] from `self` and the given lexer.

use super::combinators::{All, Any, Then, Or};
use super::primitives::Tokeniser;

/// Create combinator lexers through the builder pattern.
pub trait Chain {
    fn all(self) -> All
    where Self: Tokeniser + Sized + 'static {
        return All::new(self);
    }

    fn any(self) -> Any
    where Self: Tokeniser + Sized + 'static {
        return Any::new(self);
    }

    fn or(self, lexer: impl Tokeniser + 'static) -> Or
    where Self: Tokeniser + Sized + 'static {
        return Or::new(self).or(lexer);
    }

    fn then(self, lexer: impl Tokeniser + 'static) -> Then
    where Self: Tokeniser + Sized + 'static {
        return Then::new(self).then(lexer);
    }
}

impl<T: Tokeniser> Chain for T {}
