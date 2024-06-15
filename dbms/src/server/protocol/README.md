# Protocol

- [header](#header)
- [body](#body)

## Header

- [flags](#flags)
- [fields](#fields)

### Flags

A 64-bit set of flags indicating which fields are in the header (in the order of the flags). Each field either has fixed length or internally specifies its own length.

- [message type](#message-type)
- [serialisation version](#serialisation-version)

### Fields

#### Message type

A `u64`

- Close connection as 1
- Ack as 2 (for confirming an operation finished successfully)
- An arbitrary string as 3
- `Command` as 4
- `SqlError` as 5
- `RowSet` as 6

#### Serialisation version

- V1 as 1
- V2 as 2

...and so on for all existing versions.

## Body

Whatever was defined as [message type](#message-type) (potentially nothing), serialised with [version](#serialisation-version).
