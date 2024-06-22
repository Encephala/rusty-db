use std::str::FromStr;

use super::*;
use super::super::serialisation::Serialiser;
use super::super::types::*;
use sql_parse::parser::ColumnType;

fn new_persistence_manager() -> impl PersistenceManager {
    return FileSystem::new(
        SerialisationManager(Serialiser::V2),
        PathBuf::from("/tmp/rusty-db-tests")
    );
}

#[test]
fn create_database_path_basic() {
    let database = Database::new("db".into());

    let path = database_path(&PathBuf::from_str("/tmp").unwrap(), &database.name);

    assert_eq!(
        path,
        PathBuf::from_str("/tmp/db").unwrap()
    );
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
    );
}

#[tokio::test]
async fn save_database_basic() {
    let persistence = new_persistence_manager();

    let database = Database::new("test_save_db".into());

    persistence.save_database(&database).await.unwrap();

    let database_path = database_path(std::path::Path::new("/tmp/rusty-db-tests"), &database.name);

    assert!(std::fs::metadata(&database_path).is_ok());

    std::fs::remove_dir(&database_path).unwrap();
}
