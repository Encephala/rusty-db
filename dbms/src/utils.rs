use crate::{
    SqlError, Result,
    serialisation::Serialiser
};

pub fn serialiser_version_to_serialiser(version: u8) -> Result<Serialiser>{
    return match version {
        1 => Ok(Serialiser::V1),
        2 => Ok(Serialiser::V2),
        other => Err(SqlError::IncompatibleVersion(other)),
    };
}

#[cfg(test)]
pub mod tests {
    use sql_parse::parser::ColumnType;
    use super::super::database::{Table, Row, RowSet};
    use super::super::types::{TableName, ColumnName, DatabaseName, ColumnValue, ColumnDefinition};

    use crate::server::Runtime;
    use crate::{Database, Result, SqlError};

    impl From<&str> for TableName {
        fn from(value: &str) -> Self {
            return Self(value.into());
        }
    }

    impl From<&str> for ColumnName {
        fn from(value: &str) -> Self {
            return Self(value.into());
        }
    }

    impl From<&str> for DatabaseName {
        fn from(value: &str) -> Self {
            return Self(value.into());
        }
    }

    impl From<usize> for ColumnValue {
        fn from(value: usize) -> Self {
            return Self::Int(value);
        }
    }

    impl From<(usize, usize)> for ColumnValue {
        fn from(value: (usize, usize)) -> Self {
            return Self::Decimal(value.0, value.1);
        }
    }

    impl From<&str> for ColumnValue {
        fn from(value: &str) -> Self {
            return Self::Str(value.into());
        }
    }

    impl From<bool> for ColumnValue {
        fn from(value: bool) -> Self {
            return Self::Bool(value);
        }
    }

    pub fn test_table() -> Table {
        return Table::new(
            "test_table".into(),
            vec![
                ColumnDefinition("first".into(), ColumnType::Int),
                ColumnDefinition("second".into(), ColumnType::Bool),
            ],
        ).unwrap();
    }

    pub fn test_table_with_values() -> (Table, (Vec<ColumnValue>, Vec<ColumnValue>)) {
        let mut result = Table::new(
            "test_table".into(),
            vec![
                ColumnDefinition("first".into(), ColumnType::Int),
                ColumnDefinition("second".into(), ColumnType::Bool),
            ],
        ).unwrap();

        let row1 = vec![
            5.into(),
            true.into()
        ];

        let row2 = vec![
            6.into(),
            false.into(),
        ];

        result.insert_multiple(
            &None,
            vec![row1.clone(), row2.clone()]
        ).unwrap();

        return (result, (row1, row2));
    }

    pub fn test_row_set(values: Vec<Row>) -> Result<RowSet> {
        let types = values.first()
            .map(|row|
                row.0.iter()
                .map(|value| {
                    ColumnType::from(value)
                })
                .collect()
            )
            .ok_or(SqlError::InvalidParameter)?;

        let names = std::iter::repeat("test_column_name".to_owned())
            .map(ColumnName)
            .take(values.len())
            .collect();

        return Ok(RowSet {
            types,
            names,
            values,
        });
    }

    pub fn test_db() -> Database {
        return Database::new("test_db".into());
    }

    pub fn test_db_with_values() -> Database {
        let mut db = Database::new("test_db".into());

        let table = test_table_with_values().0;

        db.create(table).unwrap();

        return db;
    }

    pub fn test_runtime() -> Runtime {
        return Runtime::new_test();
    }

    pub fn test_runtime_with_values() -> Runtime {
        let mut runtime = Runtime::new_test();

        let db = test_db_with_values();

        runtime.create_database(db);

        return runtime;
    }
}
