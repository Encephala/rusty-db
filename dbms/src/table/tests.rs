use super::*;
use crate::utils::tests::*;

#[test]
fn insert_basic() {
    let mut table = test_table();

    let row1 = vec![
        5.into(),
        true.into(),
    ];

    let row2 = vec![
        6.into(),
        false.into(),
    ];

    table.insert(&None, row1.clone()).unwrap();

    assert_eq!(
        table.values,
        vec![test_row(row1.clone())]
    );

    table.insert_multiple(&None, vec![row1.clone(), row2.clone()]).unwrap();

    assert_eq!(
        table.values,
        vec![test_row(row1.clone()), test_row(row1), test_row(row2)]
    );
}

#[test]
fn insert_check_types() {
    let mut table = test_table();

    // Wrong order
    let row1 = vec![
        true.into(),
        5.into(),
    ];

    let row2 = vec![
        6.into(),
        "false".into(), // Wrong type
    ];

    let result1 = table.insert(&None, row1);
    let result2 = table.insert(&None, row2);

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
        Some(Where {
            left: "second".into(),
            operator: InfixOperator::Equals,
            right: true.into(),
        })
    ).unwrap();

    assert_eq!(
        where_bool_true,
        vec![Row::new(table.column_names.clone(), row1).unwrap()]
    );

    let only_int_five = table.select(
        ColumnSelector::Name(vec![ColumnName("first".into())]),
        Some(Where {
            left: "first".into(),
            operator: InfixOperator::Equals,
            right: 5.into(),
        })
    ).unwrap();

    assert_eq!(
        only_int_five,
        vec![test_row(vec![5.into()])]
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
        vec![69.into()],
        &None
    ).unwrap();

    assert_eq!(
        table.values,
        vec![
            test_row(vec![69.into(), true.into()]),
            test_row(vec![69.into(), false.into()]),
        ]
    );

    table.update(
        vec![
            ColumnName("first".into()),
        ],
        vec![420.into()],
        &Some(Where {
            left: "second".into(),
            operator: InfixOperator::Equals,
            right: true.into(),
        })
    ).unwrap();

    assert_eq!(
        table.values,
        vec![
            test_row(vec![420.into(), true.into()]),
            test_row(vec![69.into(), false.into()]),
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

    table.delete(Some(Where {
        left: "second".into(),
        operator: InfixOperator::Equals,
        right: false.into(),
    })).unwrap();

    assert_eq!(
        table.values,
        vec![
            test_row(vec![5.into(), true.into()])
        ]
    )
}
