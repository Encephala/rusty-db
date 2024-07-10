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
use crate::types::{TableName, TableSchema};
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

    async fn save_schemas(&self, database: &Database) -> Result<()>;
    async fn load_schemas(&self, database_name: &DatabaseName) -> Result<Vec<TableSchema>>;
}

fn database_path(path: &Path, name: &DatabaseName) -> PathBuf {
    let result = path.to_path_buf().join(&name.0);

    return result;
}

fn table_path(path: &Path, database_name: &DatabaseName, name: &TableName) -> PathBuf {
    let result = path.to_path_buf().join(&database_name.0).join(&name.0);

    return result;
}

fn schema_path(path: &Path, database_name: &DatabaseName) -> PathBuf {
    let result = path.to_path_buf().join(&database_name.0).join(".schema");

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

        let mut futures = vec![self.save_schemas(database)];

        // The C in ACID stands for "can't be fucked" right?
        futures.extend(
            database
                .tables
                .values()
                .map(|table| self.save_table(&database.name, table))
                .collect::<Vec<_>>(),
        );

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

        let mut schema = fs::read(path.join(".schema")).map_err(SqlError::CouldNotReadSchemas)?;
        let schemas = self.0.deserialise_schemas(schema.as_mut_slice())?;

        let futures = schemas
            .into_iter()
            .map(|schema| schema.name)
            .map(|name| self.load_table(&database.name, name))
            .collect::<Vec<_>>();

        let tables = try_join_all(futures).await?;

        tables.into_iter().for_each(|table| {
            database.tables.insert(table.schema.name.0.clone(), table);
        });

        return Ok(database);
    }

    async fn drop_database(&self, name: &DatabaseName) -> Result<()> {
        let path = database_path(&self.1, name);

        return fs::remove_dir_all(path)
            .map_err(|error| SqlError::CouldNotRemoveDatabase(name.clone(), error));
    }

    async fn save_table(&self, database_name: &DatabaseName, table: &Table) -> Result<()> {
        let path = table_path(&self.1, database_name, &table.schema.name);

        let data = self.0.serialise_table(table);

        return fs::write(path, data)
            .map_err(|error| SqlError::CouldNotStoreTable(table.schema.name.clone(), error));
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

        return fs::remove_file(path)
            .map_err(|error| SqlError::CouldNotRemoveTable(name.clone(), error));
    }

    async fn save_schemas(&self, database: &Database) -> Result<()> {
        let path = schema_path(&self.1, &database.name);

        let schemas = database
            .tables
            .values()
            .map(|table| &table.schema)
            .collect::<Vec<_>>();

        let data = self.0.serialise_schemas(schemas);

        return fs::write(path, data)
            .map_err(|error| SqlError::CouldNotStoreSchemas(database.name.clone(), error));
    }

    async fn load_schemas(&self, database_name: &DatabaseName) -> Result<Vec<TableSchema>> {
        let path = schema_path(&self.1, database_name);

        if !path.exists() {
            return Err(SqlError::SchemaDoesNotExist(database_name.clone()));
        }

        let mut data = fs::read(path).map_err(SqlError::FSError)?;

        let schemas = self.0.deserialise_schemas(data.as_mut_slice())?;

        return Ok(schemas);
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
        let result = Table::new(name, vec![], vec![]).unwrap();

        return Ok(result);
    }

    async fn drop_table(&self, _: &DatabaseName, _: &TableName) -> Result<()> {
        return Ok(());
    }

    async fn save_schemas(&self, _: &Database) -> Result<()> {
        return Ok(());
    }

    async fn load_schemas(&self, _: &DatabaseName) -> Result<Vec<TableSchema>> {
        return Ok(vec![]);
    }
}
