# Serialisation format V2

## Table
- [table name](#table-name)
- [types](#type) as [vector](#vector)
- [column names](#column-name) as [vector](#vector)
- [rows](#row) as [vector](#vector)

### Table name
- name as [string](#string)

### Type
Enum mapped to bytes (u8)
- Int as 1
- Decimal as 2
- Text as 3
- Bool as 4

### Column name
- name as [string](#string)

### Row
- [values](#column-value) as [vector](#vector)

### Column value
Length is each value is not explicitly stored (except for `Str`), but is deduced from column type when deserialising
- Int as [u64](#u64)
- Decimal whole part as [u64](#u64) then fractional part as [u64](#u64)
- Str as [string](#string)
- Bool as 0 or 1 (u8)

## Vector
- count as [u64](#u64)
- sequence of all values in order

## String
- length as [u64](#u64)
- characters as UTF8

## `u64`
- Value as little-endian bytes, fixed length of 8

## Rowset
- [names](#column-name) as [vector](#vector)
- [rows](#row) as [vector](#vector)
