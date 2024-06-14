# Serialisation format V1

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
- Int as [usize](#usize)
- Decimal whole part as [usize](#usize) then fractional part as [usize](#usize)
- Str as [string](#string)
- Bool as 0 or 1 (u8)

## Vector
- count as [usize](#usize)
- sequence of all values in order

## String
- length as [usize](#usize)
- characters as UTF8

## `usize`
- Value as little-endian bytes, length depending on `std::mem::size_of<usize>()`

## Rowset
- [types](#type) as [vector](#vector)
- [names](#column-name) as [vector](#vector)
- [rows](#row) as [vector](#vector)
