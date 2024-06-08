use std::str::FromStr;

use super::*;
use super::super::types::*;
use sql_parse::ColumnType;

#[test]
fn create_database_path_basic() {
    let database = Database::new("db".into());

    let path = database_path(&PathBuf::from_str("/tmp").unwrap(), &database.name);

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

// TODO: testing that files actually get saved to disk and stuff
// I mean idk is kinda like testing the OS but I think there's something to be gained there
