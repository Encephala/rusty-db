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
        vec![Row(row1.clone())]
    );

    table.insert_multiple(&None, vec![row1.clone(), row2.clone()]).unwrap();

    assert_eq!(
        table.values,
        vec![Row(row1.clone()), Row(row1), Row(row2)]
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
        test_row_set(vec![Row(row1.clone()), Row(row2)])
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
        test_row_set(vec![Row(row1)])
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
        test_row_set(vec![Row(vec![5.into()])])
    );

    let none = table.select(
        ColumnSelector::Name(vec![]),
        None,
    ).unwrap();

    assert_eq!(
        none,
        test_row_set(vec![
            Row(vec![]),
            Row(vec![]),
        ])
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
        None
    ).unwrap();

    assert_eq!(
        table.values,
        vec![
            Row(vec![69.into(), true.into()]),
            Row(vec![69.into(), false.into()]),
        ]
    );

    table.update(
        vec![
            ColumnName("first".into()),
        ],
        vec![420.into()],
        Some(Where {
            left: "second".into(),
            operator: InfixOperator::Equals,
            right: true.into(),
        })
    ).unwrap();

    assert_eq!(
        table.values,
        vec![
            Row(vec![420.into(), true.into()]),
            Row(vec![69.into(), false.into()]),
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
            Row(vec![5.into(), true.into()])
        ]
    )
}
