pub mod primitives;
pub mod combinators;
pub mod glue;

use primitives::Parser;
pub use primitives::{WhitespaceParser, DigitParser, LetterParser, SpecialCharParser};
pub use combinators::{AllCombinator, SomeCombinator, ThenCombinator};
pub use glue::ThenOr;

impl<T: Parser> ThenOr for T {}
