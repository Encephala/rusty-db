#[cfg(test)]
mod tests;

use std::fs::{self, DirBuilder};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::future::try_join_all;

use super::database::{Database, Table};
use super::serialisation::SerialisationManager;
use super::types::DatabaseName;
use super::SqlError;
use crate::types::TableName;
use crate::Result;

// Love me some premature abstractions
#[async_trait]
pub trait PersistenceManager: std::fmt::Debug + Send + Sync {
    async fn save_database(&self, database: &Database) -> Result<()>;
    async fn load_database(&self, name: &DatabaseName) -> Result<Database>;
    async fn drop_database(&self, name: &DatabaseName) -> Result<()>;

    async fn save_table(&self, database_name: &DatabaseName, table: &Table) -> Result<()>;
    async fn load_table(&self, database_name: &DatabaseName, name: TableName) -> Result<Table>;
    async fn drop_table(&self, database_name: &DatabaseName, name: &TableName) -> Result<()>;
}

fn database_path(path: &Path, name: &DatabaseName) -> PathBuf {
    let result = path.to_path_buf().join(&name.0);

    return result;
}

fn table_path(path: &Path, database_name: &DatabaseName, name: &TableName) -> PathBuf {
    let result = path.to_path_buf().join(&database_name.0).join(&name.0);

    return result;
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
        DirBuilder::new()
            .recursive(true)
            .mode(0o750) // Windows support can get lost byeeeee
            .create(database_path(&self.1, &database.name))
            .map_err(|error| SqlError::CouldNotStoreDatabase(database.name.clone(), error))?;

        // The C in ACID stands for "can't be fucked" right?
        let futures = database
            .tables
            .values()
            .map(|table| self.save_table(&database.name, table))
            .collect::<Vec<_>>();

        try_join_all(futures).await?;

        return Ok(());
    }

    async fn load_database(&self, name: &DatabaseName) -> Result<Database> {
        let path = database_path(&self.1, name);

        if !path.exists() {
            return Err(SqlError::DatabaseDoesNotExist(name.clone()));
        }

        // Create database object, then load tables
        let mut database = Database::new(name.clone());

        let files = fs::read_dir(path).map_err(SqlError::FSError)?;

        let mut futures = vec![];

        for file in files {
            let file = file.map_err(SqlError::FSError)?;

            let name = file
                .file_name()
                .into_string()
                .map_err(SqlError::CouldNotReadTable)?;

            let name = TableName(name);

            futures.push(self.load_table(&database.name, name));
        }

        let tables = try_join_all(futures).await?;

        tables.into_iter().for_each(|table| {
            database.tables.insert(table.name.0.clone(), table);
        });

        return Ok(database);
    }

    async fn drop_database(&self, name: &DatabaseName) -> Result<()> {
        let path = database_path(&self.1, name);

        fs::remove_dir_all(path)
            .map_err(|error| SqlError::CouldNotRemoveDatabase(name.clone(), error))?;

        return Ok(());
    }

    async fn save_table(&self, database_name: &DatabaseName, table: &Table) -> Result<()> {
        let path = table_path(&self.1, database_name, &table.name);

        let data = self.0.serialise_table(table);

        fs::write(path, data)
            .map_err(|error| SqlError::CouldNotStoreTable(table.name.clone(), error))?;

        return Ok(());
    }

    async fn load_table(&self, database_name: &DatabaseName, name: TableName) -> Result<Table> {
        let path = table_path(&self.1, database_name, &name);

        if !path.exists() {
            return Err(SqlError::TableDoesNotExist(name));
        }

        let data = fs::read(path).map_err(SqlError::FSError)?;

        let table = self.0.deserialise_table(data.as_slice())?;

        return Ok(table);
    }

    async fn drop_table(&self, database_name: &DatabaseName, name: &TableName) -> Result<()> {
        let path = table_path(&self.1, database_name, name);

        fs::remove_file(path)
            .map_err(|error| SqlError::CouldNotRemoveTable(name.clone(), error))?;

        return Ok(());
    }
}

#[cfg(test)]
#[derive(Debug)]
pub struct NoOp;

#[cfg(test)]
#[async_trait]
impl PersistenceManager for NoOp {
    async fn save_database(&self, _: &Database) -> Result<()> {
        return Ok(());
    }

    async fn load_database(&self, name: &DatabaseName) -> Result<Database> {
        return Ok(Database::new(name.clone()));
    }

    async fn drop_database(&self, _: &DatabaseName) -> Result<()> {
        return Ok(());
    }

    async fn save_table(&self, _: &DatabaseName, _: &Table) -> Result<()> {
        return Ok(());
    }

    async fn load_table(&self, _: &DatabaseName, name: TableName) -> Result<Table> {
        let result = Table::new(name, vec![]).unwrap();

        return Ok(result);
    }

    async fn drop_table(&self, _: &DatabaseName, _: &TableName) -> Result<()> {
        return Ok(());
    }
}
