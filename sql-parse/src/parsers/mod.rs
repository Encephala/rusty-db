pub mod primitives;
pub mod combinators;
pub mod chaining;
pub mod keywords;

use primitives::Parser;
pub use primitives::{Whitespace, Digit, Letter, SpecialChar};
pub use combinators::{All, Any, Or, Then};
pub use chaining::Chain;

impl<T: Parser> Chain for T {}
