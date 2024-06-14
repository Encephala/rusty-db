//! Essentially the same tests as v1, except it uses u64 instead of u64 for sizes
use super::*;

use crate::database::{Table, Row};
use sql_parse::parser::ColumnType;
use crate::types::{TableName, ColumnName, ColumnValue};
use crate::utils::tests::{test_table, test_table_with_values};

#[test]
fn serialise_column_types() {
    let types = vec![
        ColumnType::Decimal,
        ColumnType::Int,
        ColumnType::Bool,
        ColumnType::Text,
    ];

    let serialised = types.serialise();

    assert_eq!(
        serialised,
        vec![
            4, 0, 0, 0, 0, 0, 0, 0, // Length
            2, 1, 4, 3
        ]
    )
}

#[test]
fn serialise_column_names() {
    let names: Vec<ColumnName> = vec![
        "asdf".into(),
        "hello".into(),
    ];

    let serialised = names.serialise();
    assert_eq!(
        serialised,
        vec![
            2, 0, 0, 0, 0, 0, 0, 0, // Length
            4, 0, 0, 0, 0, 0, 0, 0, // Length
            97, 115, 100, 102,
            5, 0, 0, 0, 0, 0, 0, 0, // Length
            104, 101, 108, 108, 111,
        ]
    );
}

#[test]
fn serialise_column_values() {
    let values: Vec<ColumnValue> = vec![
        1.into(),
        (420, 69).into(),
        "hey".into(),
        true.into(),
        false.into(),
    ];

    let serialised = values.serialise();

    let buffer = [0_u8; 8];

    let mut expected = vec![
        5, 0, 0, 0, 0, 0, 0, 0, // Length
    ];

    expected.extend({
        let mut result = buffer;

        // Note use of little-endian bytes in serialisation::u64_to_bytes
        result[0] = 1;

        result.to_vec()
    });

    expected.extend({
        let mut result = buffer;

        // 420 = 256 + 164
        result[0] = 164;
        result[1] = 1;

        result.to_vec()
    });

    expected.extend({
        let mut result = buffer;

        result[0] = 69;

        result.to_vec()
    });

    expected.extend({
        let mut result = [0_u8; 11];

        // Set length
        result[0] = 3;

        // Characters
        result[8] = 104;
        result[9] = 101;
        result[10] = 121;

        result.to_vec()
    });

    expected.extend(vec![1, 0]);

    assert_eq!(
        serialised,
        expected
    )
}

#[test]
fn serialise_row() {
    let (_, (row1, row2)) = test_table_with_values();

    let input = &mut vec![
        Row(row1.clone()), Row(row2.clone())
    ];

    let mut expected = vec![2, 0, 0, 0, 0, 0, 0, 0];

    expected.extend(row1.serialise());
    expected.extend(row2.serialise());

    assert_eq!(
        input.serialise(),
        expected
    );

    let input: &mut Vec<Row> = &mut vec![];

    // Just the length
    let expected = vec![
        0, 0, 0, 0, 0, 0, 0, 0,
    ];

    assert_eq!(
        input.serialise(),
        expected
    );
}

#[test]
fn serialise_table() {
    let table = test_table();

    let serialised = table.serialise();

    let expected = vec![
        // Name
        10, 0, 0, 0, 0, 0, 0, 0,
        116, 101, 115, 116, 95, 116, 97, 98, 108, 101,

        // Types
        2, 0, 0, 0, 0, 0, 0, 0,
        1, 4,

        // Names
        2, 0, 0, 0, 0, 0, 0, 0,
        5, 0, 0, 0, 0, 0, 0, 0,
        102, 105, 114, 115, 116,
        6, 0, 0, 0, 0, 0, 0, 0,
        115, 101, 99, 111, 110, 100,

        // Values
        0, 0, 0, 0, 0, 0, 0, 0,
    ];

    assert_eq!(
        serialised,
        expected
    );

    let (table, _) = test_table_with_values();

    let serialised = table.serialise();

    let expected = vec![
        // Name
        10, 0, 0, 0, 0, 0, 0, 0,
        116, 101, 115, 116, 95, 116, 97, 98, 108, 101,

        // Types
        2, 0, 0, 0, 0, 0, 0, 0,
        1, 4,

        // Names
        2, 0, 0, 0, 0, 0, 0, 0,
        5, 0, 0, 0, 0, 0, 0, 0,
        102, 105, 114, 115, 116,
        6, 0, 0, 0, 0, 0, 0, 0,
        115, 101, 99, 111, 110, 100,

        // Values
        2, 0, 0, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0,
        5, 0, 0, 0, 0, 0, 0, 0,
        1,
        2, 0, 0, 0, 0, 0, 0, 0,
        6, 0, 0, 0, 0, 0, 0, 0,
        0,
    ];

    assert_eq!(
        serialised,
        expected,
    )
}

#[test]
fn deserialise_u64() {
    let input = &mut [
        1, 0, 0, 0, 0, 0, 0, 0, // 1
        164, 1, 0, 0, 0, 0, 0, 0, // 420
        0, // Too few bytes
    ].as_slice();

    assert_eq!(
        u64::deserialise(input, None.into()).unwrap(),
        1,
    );

    assert_eq!(
        u64::deserialise(input, None.into()).unwrap(),
        420,
    );

    assert!(
        u64::deserialise(input, None.into()).is_err()
    );
}

#[test]
fn deserialise_column_type() {
    let input = vec![
        ColumnType::Int,
        ColumnType::Bool,
        ColumnType::Text
    ].serialise();

    let input = &mut input.as_slice();

    // Length
    assert_eq!(
        u64::deserialise(input, None.into()).unwrap(),
        3
    );

    assert_eq!(
        ColumnType::deserialise(input, None.into()).unwrap(),
        ColumnType::Int,
    );

    assert_eq!(
        ColumnType::deserialise(input, None.into()).unwrap(),
        ColumnType::Bool,
    );

    assert_eq!(
        ColumnType::deserialise(input, None.into()).unwrap(),
        ColumnType::Text,
    );
}

#[test]
fn deserialise_table_name() {
    let input = vec![
        TableName("a".into()),
        "abcd".into(),
        "meme".into(),
    ].serialise();

    let input = &mut input.as_slice();

    // Length
    assert_eq!(
        u64::deserialise(input, None.into()).unwrap(),
        3
    );

    assert_eq!(
        TableName::deserialise(input, None.into()).unwrap(),
        "a".into()
    );

    assert_eq!(
        TableName::deserialise(input, None.into()).unwrap(),
        "abcd".into()
    );

    assert_eq!(
        TableName::deserialise(input, None.into()).unwrap(),
        "meme".into()
    );
}

#[test]
fn deserialise_column_name() {
    let input = ColumnName("hey".into()).serialise();
    let input = &mut input.as_slice();

    assert_eq!(
        ColumnName::deserialise(input, None.into()).unwrap(),
        "hey".into()
    );

    let input = ColumnName("".into()).serialise();
    let input = &mut input.as_slice();

    assert_eq!(
        ColumnName::deserialise(input, None.into()).unwrap(),
        "".into()
    );
}
#[test]
fn deserialise_vector_fixed_length_item() {
    let input = vec![
        ColumnType::Int,
        ColumnType::Bool,
        ColumnType::Text,
        ColumnType::Decimal,
    ].serialise();
    let input = &mut input.as_slice();

    assert_eq!(
        Vec::<ColumnType>::deserialise(input, None.into()).unwrap(),
        vec![
            ColumnType::Int,
            ColumnType::Bool,
            ColumnType::Text,
            ColumnType::Decimal,
        ]
    );

    // Invalid data
    let input = &mut [
        1, 0, 0, 0, 0, 0, 0, 0,
        69,
    ].as_slice();

    let result = Vec::<ColumnType>::deserialise(input, None.into());
    println!("{:?}", result);
    assert!(matches!(
        result,
        Err(SqlError::NotATypeDiscriminator(_))
    ));

    // Too short
    let input = vec![
        ColumnType::Int,
        ColumnType::Bool,
    ].serialise();
    let input = &mut input.as_slice();

    // Length
    assert_eq!(
        u64::deserialise(input, None.into()).unwrap(),
        2
    );

    assert!(Vec::<ColumnType>::deserialise(input, None.into()).is_err());
}

#[test]
fn deserialise_vector_variable_length_item() {
    let input = vec![
        ColumnName("a".into()),
        ColumnName("abc".into()),
    ].serialise();
    let input = &mut input.as_slice();

    assert_eq!(
        Vec::<ColumnName>::deserialise(input, None.into()).unwrap(),
        vec![
            ColumnName("a".into()),
            ColumnName("abc".into()),
        ]
    );
}

#[test]
fn deserialise_column_values() {
    let values = vec![
        ColumnValue::Int(1),
        (420, 69).into(),
        "hey".into(),
        true.into()
    ];
    let input = values.serialise();
    let input = &mut input.as_slice();

    assert_eq!(
        Vec::<ColumnValue>::deserialise(input, DO::ColumnTypes(vec![
            ColumnType::Int,
            ColumnType::Decimal,
            ColumnType::Text,
            ColumnType::Bool,
        ])).unwrap(),
        values
    );
}

#[test]
fn deserialise_row_vector() {
    let (_, (row1, row2)) = test_table_with_values();

    let input = vec![
        Row(row1.clone()), Row(row2.clone())
    ].serialise();
    let input = &mut input.as_slice();

    assert_eq!(
        Vec::<Row>::deserialise(input, DO::ColumnTypes(vec![
            ColumnType::Int,
            ColumnType::Bool,
        ])).unwrap(),
        vec![Row(row1), Row(row2)]
    )
}

#[test]
fn deserialise_table() {
    // We test serialise_table separately, so this is fine I guess
    let table = test_table().serialise();
    let input = &mut table.as_slice();

    let result = Table::deserialise(input, None.into()).unwrap();

    assert_eq!(
        result,
        test_table()
    );

    let table = test_table_with_values().0.serialise();
    let input = &mut table.as_slice();

    let result = Table::deserialise(input, None.into()).unwrap();

    assert_eq!(
        result,
        test_table_with_values().0
    );
}

#[test]
fn serialise_rowset() {
    let (table, _) = test_table_with_values();

    let result = table.select(
        crate::types::ColumnSelector::AllColumns,
        None,
    ).unwrap();

    let serialised = result.serialise();

    let expected = vec![
        // Types
        2, 0, 0, 0, 0, 0, 0, 0,
        1, 4,

        // Names
        2, 0, 0, 0, 0, 0, 0, 0,
        5, 0, 0, 0, 0, 0, 0, 0,
        102, 105, 114, 115, 116,
        6, 0, 0, 0, 0, 0, 0, 0,
        115, 101, 99, 111, 110, 100,

        // Values
        2, 0, 0, 0, 0, 0, 0, 0,

        2, 0, 0, 0, 0, 0, 0, 0,
        5, 0, 0, 0, 0, 0, 0, 0,
        1,

        2, 0, 0, 0, 0, 0, 0, 0,
        6, 0, 0, 0, 0, 0, 0, 0,
        0,
    ];

    assert_eq!(
        serialised,
        expected
    )
}

#[test]
fn deserialise_rowset() {
    let (table, _) = test_table_with_values();

    let result = table.select(
        crate::types::ColumnSelector::AllColumns,
        None,
    ).unwrap();

    let serialised = result.serialise();

    let deserialised = RowSet::deserialise(&mut serialised.as_slice(), DO::None).unwrap();

    assert_eq!(
        result,
        deserialised,
    );
}
