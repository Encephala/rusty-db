use std::str::FromStr;

use super::*;
use super::super::types::*;
use sql_parse::ColumnType;

#[test]
fn create_database_path_basic() {
    let database = Database::new("db".into());

    let path = database_path(&PathBuf::from_str("/tmp").unwrap(), &database);

    assert_eq!(
        path,
        PathBuf::from_str("/tmp/db").unwrap()
    )
}

#[test]
fn create_table_path_basic() {
    let mut database = Database::new("db".into());

    let table = Table::new(
        "tbl".into(),
        vec![
            ColumnDefinition("col1".into(), ColumnType::Int),
            ColumnDefinition("col2".into(), ColumnType::Bool),
        ]
    ).unwrap();

    database.create(table.clone()).unwrap();

    let path = table_path(&PathBuf::from_str("/tmp").unwrap(), &database, &table);

    assert_eq!(
        path,
        PathBuf::from_str("/tmp/db/tbl").unwrap()
    )
}

mod serialisation {
    use super::*;

    use super::super::serialisation::{SIZEOF_USIZE, usize_to_bytes};
    use crate::utils::tests::test_table;

    #[test]
    fn serialise_column_types() {
        let types = vec![
            ColumnType::Decimal,
            ColumnType::Int,
            ColumnType::Bool,
            ColumnType::Text,
        ];

        let serialised = types.serialise().unwrap();

        assert_eq!(
            serialised,
            vec![1, 0, 3, 2]
        )
    }

    #[test]
    fn serialise_column_names() {
        let names: Vec<ColumnName> = vec![
            "asdf".into(),
            "hello".into(),
        ];

        let serialised = names.serialise().unwrap();

        assert_eq!(
            serialised,
            vec![
                97, 115, 100, 102,
                104, 101, 108, 108, 111,
            ]
        );
    }

    #[allow(overflowing_literals)]
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

        let mut expected = {
            let mut result = buffer;

            // Note use of little-endian bytes in serialisation::usize_to_bytes
            result[0] = 1;

            result.to_vec()
        };

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

        expected.extend(vec![104, 101, 121]);

        expected.extend(vec![1, 0]);

        assert_eq!(
            serialised,
            expected
        )
    }

    #[test]
    fn serialise_table() {
        let table = test_table();

        let serialised = table.serialise().unwrap();

        let mut expected = vec![];

        let name_serialised = table.name.serialise().unwrap();

        expected.extend(usize_to_bytes(name_serialised.len()));
        expected.extend(name_serialised);

        let types_serialised = table.types.serialise().unwrap();

        expected.extend(usize_to_bytes(types_serialised.len()));
        expected.extend(types_serialised);

        let column_names_serialised = table.column_names.serialise().unwrap();

        expected.extend(usize_to_bytes(column_names_serialised.len()));
        expected.extend(column_names_serialised);

        let values_serialised = table.values.serialise().unwrap();

        expected.extend(usize_to_bytes(values_serialised.len()));
        expected.extend(values_serialised);

        assert_eq!(
            serialised,
            expected
        );
    }
}
