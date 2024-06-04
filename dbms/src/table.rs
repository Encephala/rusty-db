use super::{SqlError, Expression, ColumnType};

// TODO: How can I not store types on each row?
// Is it worth it?
// Make row generic on types or something? idk how to do that with arbitrary types
#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    types: Vec<ColumnType>,
    values: Vec<Expression>,
}

impl Row {
    fn new(values: Vec<Expression>) -> Self {
        let types: Vec<_> = values.iter().map(|value| value.into()).collect();

        if types.contains(&ColumnType::Invalid) {
            panic!("Tried inserting invalid type: {values:?}");
        }

        return Row {
            types,
            values,
        };
    }
}


#[derive(Debug)]
struct Table {
    types: Vec<ColumnType>,
    values: Vec<Row>,
}

impl Table {
    fn new(types: Vec<ColumnType>) -> Self {
        return Table {
            types,
            values: vec![],
        };
    }

    fn insert(&mut self, row: Row) -> Result<(), SqlError> {
        if row.types != self.types {
            return Err(SqlError::IncompatibleTypes(row.types, self.types.clone()));
        }

        self.values.push(row);

        return Ok(());
    }

    fn insert_multiple(&mut self, rows: Vec<Row>) -> Result<(), SqlError> {
        for row in rows {
            self.insert(row)?;
        }

        return Ok(());
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_basic() {
        let mut table = Table::new(vec![
            ColumnType::Int,
            ColumnType::Bool,
        ]);

        let row1 = Row::new(vec![
            Expression::IntLiteral(5),
            Expression::BoolLiteral(true),
        ]);
        let row2 = Row::new(vec![
            Expression::IntLiteral(6),
            Expression::BoolLiteral(false),
        ]);

        if let Err(message) = table.insert_multiple(vec![row1.clone(), row2.clone()]) {
            panic!("Failed to insert rows ({message:?})");
        }

        assert_eq!(
            table.values,
            vec![row1, row2]
        );
    }

    #[test]
    fn insert_check_types() {
        let mut table = Table::new(vec![
            ColumnType::Int,
            ColumnType::Bool,
        ]);

        let row1 = Row::new(vec![
            Expression::BoolLiteral(true),
            Expression::IntLiteral(5),
        ]);

        let row2 = Row::new(vec![
            Expression::IntLiteral(6),
            Expression::StrLiteral("false".into()),
        ]);

        let result1 = table.insert(row1);
        let result2 = table.insert(row2);

        assert!(result1.is_err());
        assert!(result2.is_err());

        assert_eq!(
            table.values,
            vec![]
        );
    }
}
