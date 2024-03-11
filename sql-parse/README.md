# sql-parse

A parser:

```rust
trait Parser {
    fn parse(self, input: String) -> Option<(String, String)>
}
```

i.e. it takes the characters and returns a token and the remaining characters if it could parse itself from the input,
otherwise it returns None.

That is, unless I backtrack on that idea and forget to update this readme.

### TODO
- `Identifier` parser which parses a column/table name
    - Something like an underscore in a name currently doesn't match
- Represent the result of parsers in a nicer way than just the matched string
    - Yeah I have no idea what this would ook like
- Implement parsing end of input
