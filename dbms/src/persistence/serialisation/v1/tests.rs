use super::*;

use crate::database::{Table, Row};
use sql_parse::ColumnType;
use crate::types::{TableName, ColumnName, ColumnValue};
use crate::utils::tests::{test_table, test_table_with_values};


// value -> [value, 0, 0..] to match length of usize
fn serialised_usize(value: u8) -> Vec<u8> {
    let mut result = vec![value];

    result.extend([0; SIZEOF_USIZE - 1]);

    return result;
}


#[test]
fn serialise_column_types() {
    let types = vec![
        ColumnType::Decimal,
        ColumnType::Int,
        ColumnType::Bool,
        ColumnType::Text,
    ];

    let serialised = types.serialise().unwrap();

    let mut expected = serialised_usize(4);
    expected.extend([2, 1, 4, 3]);

    assert_eq!(
        serialised,
        expected
    )
}

#[test]
fn serialise_column_names() {
    let names: Vec<ColumnName> = vec![
        "asdf".into(),
        "hello".into(),
    ];

    let serialised = names.serialise().unwrap();

    let mut expected = serialised_usize(2);
    expected.extend(serialised_usize(4));
    expected.extend([97, 115, 100, 102]);
    expected.extend(serialised_usize(5));
    expected.extend([104, 101, 108, 108, 111]);

    assert_eq!(
        serialised,
        expected
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

    let serialised = values.serialise().unwrap();

    let buffer = [0_u8; SIZEOF_USIZE];

    let mut expected = serialised_usize(5);

    expected.extend(serialised_usize(1));

    expected.extend({
        let mut result = buffer;

        // 420 = 256 + 164
        result[0] = 164;
        result[1] = 1;

        result.to_vec()
    });

    expected.extend(serialised_usize(69));

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

    let mut expected = serialised_usize(2);

    expected.extend(row1.serialise().unwrap());
    expected.extend(row2.serialise().unwrap());

    assert_eq!(
        input.serialise().unwrap(),
        expected
    );

    let input: &mut Vec<Row> = &mut vec![];

    // Just the length
    let expected = serialised_usize(0);

    assert_eq!(
        input.serialise().unwrap(),
        expected
    );
}

// TODO:
// This isn't testing shit ackshually,
// as these exact functions in this exact order are being called in the actual code
// Have to manually calculate the serialised result
#[test]
fn serialise_table() {
    let table = test_table();

    let serialised = table.serialise().unwrap();

    // Names
    let mut expected = serialised_usize(10);
    expected.extend([116, 101, 115, 116, 95, 116, 97, 98, 108, 101]);

    // Types
    expected.extend(serialised_usize(2));
    expected.extend([1, 4]);

    // Names
    expected.extend(serialised_usize(2));
    expected.extend(serialised_usize(5));
    expected.extend([102, 105, 114, 115, 116]);
    expected.extend(serialised_usize(6));
    expected.extend([115, 101, 99, 111, 110, 100]);

    // Values
    expected.extend(serialised_usize(0));

    assert_eq!(
        serialised,
        expected
    );

    let (table, _) = test_table_with_values();

    let serialised = table.serialise().unwrap();

    let mut expected = expected.get(0..expected.len() - 8).unwrap().to_vec();

    expected.extend(serialised_usize(2));
    expected.extend(serialised_usize(2));
    expected.extend(serialised_usize(5));
    expected.extend([1]);
    expected.extend(serialised_usize(2));
    expected.extend(serialised_usize(6));
    expected.extend([0]);

    assert_eq!(
        serialised,
        expected,
    )
}

#[test]
fn deserialise_usize() {
    let mut input = serialised_usize(1);
    input.extend({
        let mut buffer = [0; SIZEOF_USIZE];

        buffer[0] = 164;
        buffer[1] = 1;

        buffer
    });
    input.extend([1]);

    let input = &mut input.as_slice();

    assert_eq!(
        usize::deserialise(input, None.into()).unwrap(),
        1,
    );

    assert_eq!(
        usize::deserialise(input, None.into()).unwrap(),
        420,
    );

    assert!(
        usize::deserialise(input, None.into()).is_err()
    );
}

#[test]
fn deserialise_column_type() {
    let input = vec![
        ColumnType::Int,
        ColumnType::Bool,
        ColumnType::Text
    ].serialise().unwrap();

    let input = &mut input.as_slice();

    // Length
    assert_eq!(
        usize::deserialise(input, None.into()).unwrap(),
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
    ].serialise().unwrap();

    let input = &mut input.as_slice();

    // Length
    assert_eq!(
        usize::deserialise(input, None.into()).unwrap(),
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
    let input = ColumnName("hey".into()).serialise().unwrap();
    let input = &mut input.as_slice();

    assert_eq!(
        ColumnName::deserialise(input, None.into()).unwrap(),
        "hey".into()
    );

    let input = ColumnName("".into()).serialise().unwrap();
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
    ].serialise().unwrap();
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
    let mut input = serialised_usize(1);
    input.extend([69]);

    let result = Vec::<ColumnType>::deserialise(&mut input.as_slice(), None.into());
    dbg!(&result);
    assert!(matches!(
        result,
        Err(SqlError::NotATypeDiscriminator(_))
    ));

    // Too short
    let input = vec![
        ColumnType::Int,
        ColumnType::Bool,
    ].serialise().unwrap();
    let input = &mut input.as_slice();

    // Length
    assert_eq!(
        usize::deserialise(input, None.into()).unwrap(),
        2
    );

    assert!(Vec::<ColumnType>::deserialise(input, None.into()).is_err());
}

#[test]
fn deserialise_vector_variable_length_item() {
    let input = vec![
        ColumnName("a".into()),
        ColumnName("abc".into()),
    ].serialise().unwrap();
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
    let input = values.serialise().unwrap();
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
    ].serialise().unwrap();
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
    let table = test_table().serialise().unwrap();
    let input = &mut table.as_slice();

    let result = Table::deserialise(input, None.into()).unwrap();

    assert_eq!(
        result,
        test_table()
    );

    let table = test_table_with_values().0.serialise().unwrap();
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

    let serialised = result.serialise().unwrap();

    // Names
    let mut expected = serialised_usize(2);
    expected.extend(serialised_usize(5));
    expected.extend([102, 105, 114, 115, 116]);
    expected.extend(serialised_usize(6));
    expected.extend([115, 101, 99, 111, 110, 100]);

    // Values
    expected.extend(serialised_usize(2));
    expected.extend(serialised_usize(2));
    expected.extend(serialised_usize(5));
    expected.extend([1]);
    expected.extend(serialised_usize(2));
    expected.extend(serialised_usize(6));
    expected.extend([0]);

    assert_eq!(
        serialised,
        expected
    )
}