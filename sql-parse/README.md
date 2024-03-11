# sql-parse

A parser:

```rs
trait Parser {
    fn parse(self, input: String) -> Option<(Vec<Token>, String)>
}
```

i.e. it takes the characters and returns (a) token(s) and the remaining characters if it could parse itself
from the input, otherwise it returns None.

That is, unless I backtrack on that idea and forget to update this readme.

### TODO
- Represent the result of parsers in a nicer way than just the matched string
    - Yeah I have no idea what this would ook like
