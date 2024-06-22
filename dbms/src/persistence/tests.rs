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
        let (persistence, path) = new_filesystem_manager();

        let database = Database::new("test_save_db".into());

        persistence.save_database(&database).await.unwrap();

        let db_path = path.join(&database.name.0);

        assert!(std::fs::metadata(db_path).is_ok());
    }

    #[tokio::test]
    async fn save_database_duplicate() {
        let persistence = new_filesystem_manager().0;

        let database = Database::new("test_save_duplicate_db".into());

        persistence.save_database(&database).await.unwrap();

        let result = persistence.save_database(&database).await;

        if let Err(SqlError::DuplicateDatabase(name)) = result {
            assert_eq!(
                name,
                database.name,
            );
        } else {
            panic!("Wrong result type");
        }
    }

    #[tokio::test]
    async fn load_database_basic() {
        let db = test_db_with_values();

        let persistence = new_filesystem_manager().0;

        persistence.save_database(&db).await.unwrap();

        let result = persistence.load_database(&db.name).await.unwrap();

        assert_eq!(
            result,
            db
        );
    }

    #[tokio::test]
    async fn load_database_nonexistent() {
        let persistence = new_filesystem_manager().0;

        let result = persistence.load_database(&"nonexistent".into()).await;

        if let Err(SqlError::DatabaseDoesNotExist(name)) = result {
            assert_eq!(
                name,
                "nonexistent".into()
            );
        } else {
            panic!("Wrong result type")
        }
    }

    #[tokio::test]
    async fn drop_database_basic() {
        let (persistence, ref path) = new_filesystem_manager();

        let db = test_db();

        persistence.save_database(&db).await.unwrap();

        let db_path = path.join(&db.name.0);

        // Created db
        assert!(std::fs::metadata(&db_path).is_ok());

        persistence.drop_database(&db.name).await.unwrap();

        // Dropped db
        if let Err(error) = std::fs::metadata(db_path) {
            let message = format!("{error:?}");

            dbg!(&message);
            assert!(
                message.contains("kind: NotFound")
            );
        } else {
            panic!("Path exists");
        }
    }

    #[tokio::test]
    async fn drop_database_nonexistent() {
        let persistence = new_filesystem_manager().0;

        let result = persistence.drop_database(&"nonexistent".into()).await;

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
            panic!("Wrong result type")
        }
    }
}
