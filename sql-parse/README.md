# sql-parse

A parser:

```rust
trait Parser {
    fn parse(self, input: &str) -> Option<(Token, &str)>
}
```

i.e. it takes the characters and returns a token and the remaining characters if it could parse itself from the input,
otherwise it returns None.

That is, unless I backtrack on that idea and forget to update this readme.
