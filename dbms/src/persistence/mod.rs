#[cfg(test)]
mod tests;

mod serialisation;

use std::fs::{remove_dir_all, remove_file, DirBuilder, File};
use std::io::Write;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

use serialisation::Serialise;

use super::SqlError;
use super::database::{Database, Table};

// Love me some premature abstractions
pub trait PersistenceManager {
    fn save_database(&mut self, database: &Database) -> Result<(), SqlError>;
    fn delete_database(&mut self, database: &Database) -> Result<(), SqlError>;

    fn save_table(&mut self, database: &Database, table: &Table) -> Result<(), SqlError>;
    fn delete_table(&mut self, database: &Database, table: &Table) -> Result<(), SqlError>;

    fn load(&mut self) -> Result<(), SqlError>;
}

pub struct FileSystem(pub PathBuf);

impl PersistenceManager for FileSystem {
    fn save_database(&mut self, database: &Database) -> Result<(), SqlError> {
        DirBuilder::new()
            .recursive(true)
            .mode(0o550) // Windows support can get lost byeeeee
            .create(database_path(&self.0, database))
            .map_err(|error| SqlError::CouldNotStoreDatabase(database.name.clone(), error))?;

        for (_, table) in database.tables.iter() {
            // The C in ACID stands for "can't be fucked" right?
            self.save_table(database, table)?;
        }

        return Ok(());
    }

    fn delete_database(&mut self, database: &Database) -> Result<(), SqlError> {
        let path = database_path(&self.0, database);

        remove_dir_all(path)
            .map_err(|error| SqlError::CouldNotRemoveDatabase(database.name.clone(), error))?;

        return Ok(());
    }

    fn save_table(&mut self, database: &Database, table: &Table) -> Result<(), SqlError> {
        let path = table_path(&self.0, database, table);

        let data = table.serialise()?;

        let mut file = File::open(path)
            .map_err(|error| SqlError::CouldNotStoreTable(table.name.clone(), error))?;

        file.write(&data)
            .map_err(|error| SqlError::CouldNotStoreTable(table.name.clone(), error))?;

        return Ok(());
    }

    fn delete_table(&mut self, database: &Database, table: &Table) -> Result<(), SqlError> {
        let path = table_path(&self.0, database, table);

        remove_file(path)
            .map_err(|error| SqlError::CouldNotRemoveTable(table.name.clone(), error))?;

        return Ok(());
    }

    fn load(&mut self) -> Result<(), SqlError> {
        todo!()
    }
}

fn database_path(path: &Path, database: &Database) -> PathBuf {
    let mut result = path.to_path_buf();

    result.push(&database.name.0);

    return result;
}

fn table_path(path: &Path, database: &Database, table: &Table) -> PathBuf {
    let mut result = path.to_path_buf();

    result.push(&database.name.0);
    result.push(&table.name.0);

    return result;
}
