use std::str::FromStr;

use super::*;
use super::super::serialisation::Serialiser;
use super::super::types::*;
use sql_parse::parser::ColumnType;

mod filesystem {
    use crate::utils::tests::*;

    use super::*;

    const TEST_PATH: &str = "/tmp/rusty-db-tests/";

    fn new_filesystem_manager() -> (FileSystem, PathBuf) {
        let time_since_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();

        // Very random yes
        let random_suffix = time_since_epoch.subsec_nanos().to_string();

        let path = PathBuf::from(TEST_PATH.to_owned() + &random_suffix);

        let manager = FileSystem::new(
            SerialisationManager(Serialiser::V2),
            path.clone()
        );

        println!("Test running at path {path:?}");

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

        let path = table_path(&PathBuf::from_str("/tmp").unwrap(), &database.name, &table.name);

        assert_eq!(
            path,
            PathBuf::from_str("/tmp/db/tbl").unwrap()
        );
    }

    #[tokio::test]
    async fn save_database_basic() {
        let (persistence_manager, ref path) = new_filesystem_manager();

        let database = Database::new("test_save_db".into());

        persistence_manager.save_database(&database).await.unwrap();

        let db_path = database_path(path, &database.name);

        assert!(db_path.exists());
    }

    #[tokio::test]
    async fn save_database_duplicate() {
        let persistence_manager = new_filesystem_manager().0;

        let database = Database::new("test_save_duplicate_db".into());

        persistence_manager.save_database(&database).await.unwrap();

        let result = persistence_manager.save_database(&database).await;

        if let Err(SqlError::DuplicateDatabase(name)) = result {
            assert_eq!(
                name,
                database.name,
            );
        } else {
            panic!("Wrong result type: {result:?}");
        }
    }

    #[tokio::test]
    async fn load_database_basic() {
        let db = test_db_with_values();

        let persistence_manager = new_filesystem_manager().0;

        persistence_manager.save_database(&db).await.unwrap();

        let result = persistence_manager.load_database(&db.name).await.unwrap();

        assert_eq!(
            result,
            db
        );
    }

    #[tokio::test]
    async fn load_database_nonexistent() {
        let persistence_manager = new_filesystem_manager().0;

        let result = persistence_manager.load_database(&"nonexistent".into()).await;

        if let Err(SqlError::DatabaseDoesNotExist(name)) = result {
            assert_eq!(
                name,
                "nonexistent".into()
            );
        } else {
            panic!("Wrong result type: {result:?}")
        }
    }

    #[tokio::test]
    async fn drop_database_basic() {
        let (persistence_manager, ref path) = new_filesystem_manager();

        let db = test_db();

        persistence_manager.save_database(&db).await.unwrap();

        let db_path = database_path(path, &db.name);

        assert!(db_path.exists());

        persistence_manager.drop_database(&db.name).await.unwrap();

        assert!(!db_path.exists());
    }

    #[tokio::test]
    async fn drop_database_nonexistent() {
        let persistence_manager = new_filesystem_manager().0;

        let result = persistence_manager.drop_database(&"nonexistent".into()).await;

        if let Err(SqlError::CouldNotRemoveDatabase(name, error)) = result {
            assert_eq!(
                name,
                "nonexistent".into()
            );

            let message = format!("{error:?}");

            dbg!(&message);
            assert!(
                message.contains("kind: NotFound")
            );
        } else {
            panic!("Wrong result type: {result:?}")
        }
    }

    #[tokio::test]
    async fn save_table_basic() {
        let (persistence_manager, ref path) = new_filesystem_manager();

        let db = test_db();

        let (table, _) = test_table_with_values();

        persistence_manager.save_database(&db).await.unwrap();

        persistence_manager.save_table(&db.name, &table).await.unwrap();

        let table_path = table_path(path, &db.name, &table.name);

        assert!(table_path.exists());
    }

    #[tokio::test]
    async fn load_table_basic() {
        let persistence_manager = new_filesystem_manager().0;

        let db = test_db_with_values();

        persistence_manager.save_database(&db).await.unwrap();

        let result = persistence_manager.load_table(&db.name, &"test_table".into()).await.unwrap();

        assert_eq!(
            result,
            test_table_with_values().0
        );
    }

    #[tokio::test]
    async fn load_table_nonexistent() {
        let persistence_manager = new_filesystem_manager().0;

        let db = test_db_with_values();

        persistence_manager.save_database(&db).await.unwrap();

        let result = persistence_manager.load_table(&db.name, &"nonexistent".into()).await;

        if let Err(SqlError::TableDoesNotExist(name)) = result {
            assert_eq!(
                name,
                "nonexistent".into()
            )
        } else {
            panic!("Wrong result type: {result:?}");
        }
    }

    #[tokio::test]
    async fn drop_table_basic() {
        let (persistence_manager, ref path) = new_filesystem_manager();

        let db = test_db_with_values();

        persistence_manager.save_database(&db).await.unwrap();

        let table_path = table_path(path, &db.name, &"test_table".into());

        assert!(table_path.exists());

        persistence_manager.drop_table(&db.name, &"test_table".into()).await.unwrap();

        assert!(!table_path.exists());
    }
}
