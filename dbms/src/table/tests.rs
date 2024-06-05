use super::*;
use crate::utils::tests::{test_table, test_table_with_values, test_row};

#[test]
fn insert_basic() {
    let mut table = test_table();

    let row1 = vec![
        ColumnValue::Int(5),
        ColumnValue::Bool(true),
    ];

    let row2 = vec![
        ColumnValue::Int(6),
        ColumnValue::Bool(false),
    ];

    table.insert(row1.clone()).unwrap();

    assert_eq!(
        table.values,
        vec![test_row(row1.clone())]
    );

    table.insert_multiple(vec![row1.clone(), row2.clone()]).unwrap();

    assert_eq!(
        table.values,
        vec![test_row(row1.clone()), test_row(row1), test_row(row2)]
    );
}

#[test]
fn insert_check_types() {
    let mut table = test_table();

    let row1 = vec![
        ColumnValue::Bool(true),
        ColumnValue::Int(5),
    ];

    let row2 = vec![
        ColumnValue::Int(6),
        ColumnValue::Str("false".into()),
    ];

    let result1 = table.insert(row1);
    let result2 = table.insert(row2);

    assert!(matches!(result1, Err(SqlError::IncompatibleTypes(_, _))));
    assert!(matches!(result2, Err(SqlError::IncompatibleTypes(_, _))));

    assert_eq!(
        table.values,
        vec![]
    );
}

#[test]
fn select_basic() {
    let (table, (row1, row2)) = test_table_with_values();

    let all = table.select(ColumnSelector::AllColumns, None).unwrap();

    assert_eq!(
        all,
        vec![Row::new(table.column_names.clone(), row1.clone()).unwrap(), Row::new(table.column_names.clone(), row2).unwrap()]
    );

    let where_bool_true = table.select(
        ColumnSelector::AllColumns,
        Some(Expression::Where {
            left: Expression::Ident("second".into()).into(),
            operator: InfixOperator::Equals,
            right: Expression::Bool(true).into(),
        })
    ).unwrap();

    assert_eq!(
        where_bool_true,
        vec![Row::new(table.column_names.clone(), row1).unwrap()]
    );

    let only_int_five = table.select(
        ColumnSelector::Name(vec![ColumnName("first".into())]),
        Some(Expression::Where {
            left: Expression::Ident("first".into()).into(),
            operator: InfixOperator::Equals,
            right: Expression::Int(5).into(),
        })
    ).unwrap();

    assert_eq!(
        only_int_five,
        vec![test_row(vec![ColumnValue::Int(5)])]
    );

    let none = table.select(
        ColumnSelector::Name(vec![]),
        None,
    ).unwrap();

    assert_eq!(
        none,
        vec![
            Row::new(vec![], vec![]).unwrap(),
            Row::new(vec![], vec![]).unwrap(),
        ]
    )
}

#[test]
fn update_basic() {
    let (mut table, _) = test_table_with_values();

    table.update(
        vec![
            ColumnName("first".into()),
        ],
        vec![ColumnValue::Int(69)],
        None
    ).unwrap();

    assert_eq!(
        table.values,
        vec![
            test_row(vec![ColumnValue::Int(69), ColumnValue::Bool(true)]),
            test_row(vec![ColumnValue::Int(69), ColumnValue::Bool(false)]),
        ]
    );

    table.update(
        vec![
            ColumnName("first".into()),
        ],
        vec![ColumnValue::Int(420)],
        Some(Expression::Where {
            left: Expression::Ident("second".into()).into(),
            operator: InfixOperator::Equals,
            right: Expression::Bool(true).into(),
        })
    ).unwrap();

    assert_eq!(
        table.values,
        vec![
            test_row(vec![ColumnValue::Int(420), ColumnValue::Bool(true)]),
            test_row(vec![ColumnValue::Int(69), ColumnValue::Bool(false)]),
        ]
    );
}

#[test]
fn delete_basic() {
    let (mut table, _) = test_table_with_values();

    table.delete(None).unwrap();

    assert_eq!(
        table.values,
        vec![]
    );

    let (mut table, _) = test_table_with_values();

    table.delete(Some(Expression::Where {
        left: Expression::Ident("second".into()).into(),
        operator: InfixOperator::Equals,
        right: Expression::Bool(false).into(),
    })).unwrap();

    assert_eq!(
        table.values,
        vec![
            test_row(vec![ColumnValue::Int(5), ColumnValue::Bool(true)])
        ]
    )
}
