#[cfg(test)]
mod tests;

use std::fs::{remove_dir_all, remove_file, DirBuilder, write, read_dir};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::future::try_join_all;

use crate::Result;
use super::SqlError;
use super::database::{Database, Table};
use super::types::DatabaseName;
use super::serialisation::SerialisationManager;

// Love me some premature abstractions
#[async_trait]
pub trait PersistenceManager: std::fmt::Debug + Send + Sync {
    async fn save_database(&self, database: &Database) -> Result<()>;
    async fn delete_database(&self, name: DatabaseName) -> Result<DatabaseName>;

    async fn save_table(&self, database: &Database, table: &Table) -> Result<()>;
    async fn delete_table(&self, database: &Database, table: &Table) -> Result<()>;

    async fn load_database(&self, database_name: DatabaseName) -> Result<Database>;
}

#[derive(Debug)]
pub struct FileSystem(SerialisationManager, PathBuf);

impl FileSystem {
    pub fn new(serialiser: SerialisationManager, path: PathBuf) -> Self {
        return Self(serialiser, path);
    }
}

#[async_trait]
impl PersistenceManager for FileSystem {
    async fn save_database(&self, database: &Database) -> Result<()> {
        let db_path = database_path(&self.1, &database.name);

        let exists = std::fs::metadata(&db_path).is_ok();
        if exists {
            return Err(SqlError::DuplicateDatabase(db_path.to_string_lossy().into()));
        }

        DirBuilder::new()
            .recursive(true)
            .mode(0o750) // Windows support can get lost byeeeee
            .create(database_path(&self.1, &database.name))
            .map_err(|error| SqlError::CouldNotStoreDatabase(database.name.clone(), error))?;

        // The C in ACID stands for "can't be fucked" right?
        let futures = database.tables.values()
            .map(|table| self.save_table(database, table))
            .collect::<Vec<_>>();

        try_join_all(futures).await?;

        return Ok(());
    }

    async fn delete_database(&self, name: DatabaseName) -> Result<DatabaseName> {
        let path = database_path(&self.1, &name);

        remove_dir_all(path)
            .map_err(|error| SqlError::CouldNotRemoveDatabase(name.clone(), error))?;

        return Ok(name);
    }

    async fn save_table(&self, database: &Database, table: &Table) -> Result<()> {
        let path = table_path(&self.1, database, table);

        let data = self.0.serialise_table(table);

        write(path, data)
            .map_err(|error| SqlError::CouldNotStoreTable(table.name.clone(), error))?;

        return Ok(());
    }

    async fn delete_table(&self, database: &Database, table: &Table) -> Result<()> {
        let path = table_path(&self.1, database, table);

        remove_file(path)
            .map_err(|error| SqlError::CouldNotRemoveTable(table.name.clone(), error))?;

        return Ok(());
    }

    async fn load_database(&self, database_name: DatabaseName) -> Result<Database> {
        let path = database_path(&self.1, &database_name);

        if !path.exists() {
            return Err(SqlError::DatabaseDoesNotExist(database_name));
        }

        // Create database object, then load tables
        let mut database = Database::new(database_name);

        let files = read_dir(path).map_err(SqlError::FSError)?;

        for file in files {
            let file = file.map_err(SqlError::FSError)?;

            let data = std::fs::read(file.path())
                .map_err(SqlError::FSError)?;

            let table = self.0.deserialise_table(data.as_slice())?;

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
