pub mod primitives;
pub mod combinators;
pub mod chaining;
pub mod keywords;

pub use primitives::{Parser, Whitespace, Digit, Letter, Literal};
pub use combinators::{All, Any, Or, Then};
pub use chaining::Chain;
pub use keywords::Keyword;

impl<T: Parser> Chain for T {}
