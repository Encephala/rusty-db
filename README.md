# rusty-db
Useless DBMS, just to learn the following:

- ~~Parsing (SQL in this case)~~
- CLI
- Serialisation/deserialisation
- Manual TCP connections?
    - Even websockets or something for shits and giggles?


### Parser

#### TODO

- AND/OR/NOT
- DISTINCT
- JOIN
- Some builtins like MIN, MAX, and also COUNT
- GROUP BY? (lot of effort I think)
- Constraints
- INDEX?

#### Notes

- The fact that parsing fails through `None` without giving a reason kinda sucks for the dev experience.
Guess I'll have to use `Result<Option>` or `Option<Result>`.

### DBMS
