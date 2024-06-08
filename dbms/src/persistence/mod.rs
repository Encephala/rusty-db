#[cfg(test)]
mod tests;

mod serialisation;

use std::fs::{remove_dir_all, remove_file, DirBuilder, write, read_dir};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

use serialisation::{Deserialise, Serialise};

use super::SqlError;
use super::database::{Database, Table};
use super::types::DatabaseName;

// Love me some premature abstractions
pub trait PersistenceManager {
    fn save_database(&self, database: &Database) -> Result<(), SqlError>;
    fn delete_database(&self, database: &Database) -> Result<(), SqlError>;

    fn save_table(&self, database: &Database, table: &Table) -> Result<(), SqlError>;
    fn delete_table(&self, database: &Database, table: &Table) -> Result<(), SqlError>;

    fn load_database(&self, database_name: DatabaseName) -> Result<Database, SqlError>;
}

#[derive(Debug)]
pub struct FileSystem(PathBuf);

impl FileSystem {
    pub fn new(path: PathBuf) -> Self {
        return Self(path);
    }
}

impl PersistenceManager for FileSystem {
    fn save_database(&self, database: &Database) -> Result<(), SqlError> {
        DirBuilder::new()
            .recursive(true)
            .mode(0o750) // Windows support can get lost byeeeee
            .create(database_path(&self.0, &database.name))
            .map_err(|error| SqlError::CouldNotStoreDatabase(database.name.clone(), error))?;

        for table in database.tables.values() {
            // The C in ACID stands for "can't be fucked" right?
            self.save_table(database, table)?;
        }

        return Ok(());
    }

    fn delete_database(&self, database: &Database) -> Result<(), SqlError> {
        let path = database_path(&self.0, &database.name);

        remove_dir_all(path)
            .map_err(|error| SqlError::CouldNotRemoveDatabase(database.name.clone(), error))?;

        return Ok(());
    }

    fn save_table(&self, database: &Database, table: &Table) -> Result<(), SqlError> {
        let path = table_path(&self.0, database, table);

        let data = table.serialise()?;

        write(path, data)
            .map_err(|error| SqlError::CouldNotStoreTable(table.name.clone(), error))?;

        return Ok(());
    }

    fn delete_table(&self, database: &Database, table: &Table) -> Result<(), SqlError> {
        let path = table_path(&self.0, database, table);

        remove_file(path)
            .map_err(|error| SqlError::CouldNotRemoveTable(table.name.clone(), error))?;

        return Ok(());
    }

    fn load_database(&self, database_name: DatabaseName) -> Result<Database, SqlError> {
        let path = database_path(&self.0, &database_name);

        if !path.exists() {
            return Err(SqlError::DatabaseDoesNotExist(database_name));
        }

        let mut database = Database::new(database_name);

        let files = read_dir(path).map_err(SqlError::FSError)?;

        for file in files {
            let file = file.map_err(SqlError::FSError)?;

            let data = std::fs::read(file.path())
                .map_err(SqlError::FSError)?;

            let table = Table::deserialise(&mut data.as_slice(), None.into())?;

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
