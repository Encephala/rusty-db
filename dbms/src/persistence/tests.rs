use std::str::FromStr;

use super::*;
use super::super::serialisation::Serialiser;
use super::super::types::*;
use sql_parse::parser::ColumnType;

mod filesystem {
    use super::*;

    const TEST_PATH: &str = "/tmp/rusty-db-tests/";

    fn new_persistence_manager() -> (impl PersistenceManager, PathBuf) {
        let time_since_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();

        // Very random yes
        let random_suffix = time_since_epoch.subsec_nanos().to_string();

        let path = PathBuf::from(TEST_PATH.to_owned() + &random_suffix);

        let manager = FileSystem::new(
            SerialisationManager(Serialiser::V2),
            PathBuf::from(&path)
        );

        return (manager, path);
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
        let (persistence, path) = new_persistence_manager();

        let database = Database::new("test_save_db".into());

        persistence.save_database(&database).await.unwrap();

        assert!(std::fs::metadata(&path).is_ok());
    }
}
