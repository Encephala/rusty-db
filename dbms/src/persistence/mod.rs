mod serialisation;

use std::fs::{remove_dir_all, remove_file, DirBuilder, write, read_dir};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

use serialisation::Serialiser;

use super::SqlError;
use super::database::{Database, Table};
use super::types::DatabaseName;

pub use serialisation::V1;

// Love me some premature abstractions
pub trait PersistenceManager {
    fn save_database(&self, database: &Database) -> Result<(), SqlError>;
    fn delete_database(&self, database: &Database) -> Result<(), SqlError>;

    fn save_table(&self, database: &Database, table: &Table) -> Result<(), SqlError>;
    fn delete_table(&self, database: &Database, table: &Table) -> Result<(), SqlError>;

    fn load_database(&self, database_name: DatabaseName) -> Result<Database, SqlError>;
}

#[derive(Debug)]
pub struct FileSystem<S: Serialiser>(S, PathBuf);

impl<S: Serialiser> FileSystem<S> {
    pub fn new(serialiser: S, path: PathBuf) -> Self {
        return Self(serialiser, path);
    }
}

impl<S: Serialiser> PersistenceManager for FileSystem<S> {
    fn save_database(&self, database: &Database) -> Result<(), SqlError> {
        DirBuilder::new()
            .recursive(true)
            .mode(0o750) // Windows support can get lost byeeeee
            .create(database_path(&self.1, &database.name))
            .map_err(|error| SqlError::CouldNotStoreDatabase(database.name.clone(), error))?;

        for table in database.tables.values() {
            // The C in ACID stands for "can't be fucked" right?
            self.save_table(database, table)?;
        }

        return Ok(());
    }

    fn delete_database(&self, database: &Database) -> Result<(), SqlError> {
        let path = database_path(&self.1, &database.name);

        remove_dir_all(path)
            .map_err(|error| SqlError::CouldNotRemoveDatabase(database.name.clone(), error))?;

        return Ok(());
    }

    fn save_table(&self, database: &Database, table: &Table) -> Result<(), SqlError> {
        let path = table_path(&self.1, database, table);

        let data = self.0.serialise_table(table)?;

        write(path, data)
            .map_err(|error| SqlError::CouldNotStoreTable(table.name.clone(), error))?;

        return Ok(());
    }

    fn delete_table(&self, database: &Database, table: &Table) -> Result<(), SqlError> {
        let path = table_path(&self.1, database, table);

        remove_file(path)
            .map_err(|error| SqlError::CouldNotRemoveTable(table.name.clone(), error))?;

        return Ok(());
    }

    fn load_database(&self, database_name: DatabaseName) -> Result<Database, SqlError> {
        let path = database_path(&self.1, &database_name);

        if !path.exists() {
            return Err(SqlError::DatabaseDoesNotExist(database_name));
        }

        let mut database = Database::new(database_name);

        let files = read_dir(path).map_err(SqlError::FSError)?;

        for file in files {
            let file = file.map_err(SqlError::FSError)?;

            let data = std::fs::read(file.path())
                .map_err(SqlError::FSError)?;

            let table = self.0.deserialise_table(&mut data.as_slice())?;

            database.tables.insert(table.name.0.clone(), table);
        }

        return Ok(database);
    }
}

fn database_path(path: &Path, database: &DatabaseName) -> PathBuf {
    let mut result = path.to_path_buf();

    result.push(&database.0);

    return result;
}

fn table_path(path: &Path, database: &Database, table: &Table) -> PathBuf {
    let mut result = path.to_path_buf();

    result.push(&database.name.0);
    result.push(&table.name.0);

    return result;
}

#[cfg(test)]
mod tests{
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
}
