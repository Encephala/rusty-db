pub mod primitives;
pub mod combinators;
pub mod chaining;

use primitives::Parser;
pub use primitives::{WhitespaceParser, DigitParser, LetterParser, SpecialCharParser};
pub use combinators::{AllCombinator, SomeCombinator, ThenCombinator};
pub use chaining::CombinatorChain;

impl<T: Parser> CombinatorChain for T {}
