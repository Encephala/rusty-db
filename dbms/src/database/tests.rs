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
        test_row_set(vec![Row(row1.clone()), Row(row2)]).unwrap()
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
        test_row_set(vec![Row(row1)]).unwrap()
    );
}

#[test]
fn evaluate_equal() {
    use ColumnValue::*;

    let row1 = Row(vec![Int(5), Bool(true)]);

    let inputs = [
        (row1.evaluate_equal(0, &Int(5)).unwrap(), true),
        (row1.evaluate_equal(0, &Int(6)).unwrap(), false),
        (row1.evaluate_equal(0, &Decimal(5, 0)).unwrap(), true),
        (row1.evaluate_equal(0, &Decimal(5, 1)).unwrap(), false),
    ];

    inputs.iter().for_each(|(result, expected)| {
        assert_eq!(result, expected);
    });

    assert!(matches!(
        row1.evaluate_equal(0, &Bool(true)),
        Err(SqlError::ImpossibleComparison(Int(5), Bool(true)))
    ));
}

#[test]
fn evaluate_not_equal() {
    use ColumnValue::*;

    let row1 = Row(vec![Int(5), Bool(true)]);

    let inputs = [
        (row1.evaluate_not_equal(0, &Int(5)).unwrap(), false),
        (row1.evaluate_not_equal(0, &Int(6)).unwrap(), true),
        (row1.evaluate_not_equal(0, &Decimal(5, 0)).unwrap(), false),
        (row1.evaluate_not_equal(0, &Decimal(5, 1)).unwrap(), true),
    ];

    inputs.iter().for_each(|(result, expected)| {
        assert_eq!(result, expected);
    });

    assert!(matches!(
        row1.evaluate_not_equal(0, &Bool(true)),
        Err(SqlError::ImpossibleComparison(Int(5), Bool(true)))
    ));
}

#[test]
fn evaluate_less_than() {
    use ColumnValue::*;

    let row1 = Row(vec![Int(5), Bool(true)]);
    let row2 = Row(vec![Int(6), Bool(false)]);

    let inputs = [
        (row1.evaluate_less_than(0, &Int(5)).unwrap(), false),
        (row1.evaluate_less_than(0, &Int(6)).unwrap(), true),
        (row1.evaluate_less_than(0, &Int(4)).unwrap(), false),
        (row1.evaluate_less_than(0, &Decimal(5, 0)).unwrap(), false),
        (row1.evaluate_less_than(1, &Bool(false)).unwrap(), false),
        (row1.evaluate_less_than(1, &Bool(true)).unwrap(), false),

        (row2.evaluate_less_than(1, &Bool(true)).unwrap(), true),
    ];

    inputs.iter().for_each(|(result, expected)| {
        assert_eq!(result, expected);
    });

    let failing_inputs = [
        (row1.evaluate_less_than(1, &Decimal(5, 0))),
        (row1.evaluate_less_than(0, &Bool(false))),
    ];

    failing_inputs.iter().for_each(|input| {
        assert!(matches!(input, Err(SqlError::ImpossibleComparison(_, _))))
    });
}

#[test]
fn evaluate_less_than_equal() {
    use ColumnValue::*;

    let row1 = Row(vec![Int(5), Bool(true)]);
    let row2 = Row(vec![Int(6), Bool(false)]);

    let inputs = [
        (row1.evaluate_less_than_equal(0, &Int(5)).unwrap(), true),
        (row1.evaluate_less_than_equal(0, &Int(6)).unwrap(), true),
        (row1.evaluate_less_than_equal(0, &Int(4)).unwrap(), false),
        (row1.evaluate_less_than_equal(0, &Decimal(5, 0)).unwrap(), true),
        (row1.evaluate_less_than_equal(1, &Bool(false)).unwrap(), false),
        (row1.evaluate_less_than_equal(1, &Bool(true)).unwrap(), true),

        (row2.evaluate_less_than_equal(1, &Bool(true)).unwrap(), true),
    ];

    inputs.iter().for_each(|(result, expected)| {
        assert_eq!(result, expected);
    });

    let failing_inputs = [
        (row1.evaluate_less_than_equal(1, &Decimal(5, 0))),
        (row1.evaluate_less_than_equal(0, &Bool(false))),
    ];

    failing_inputs.iter().for_each(|input| {
        assert!(matches!(input, Err(SqlError::ImpossibleComparison(_, _))))
    });
}

#[test]
fn evaluate_greater_than() {
    use ColumnValue::*;

    let row1 = Row(vec![Int(5), Bool(true)]);
    let row2 = Row(vec![Int(6), Bool(false)]);

    let inputs = [
        (row1.evaluate_greater_than(0, &Int(5)).unwrap(), false),
        (row1.evaluate_greater_than(0, &Int(6)).unwrap(), false),
        (row1.evaluate_greater_than(0, &Int(4)).unwrap(), true),
        (row1.evaluate_greater_than(0, &Decimal(5, 0)).unwrap(), false),
        (row1.evaluate_greater_than(1, &Bool(false)).unwrap(), true),
        (row1.evaluate_greater_than(1, &Bool(true)).unwrap(), false),

        (row2.evaluate_greater_than(1, &Bool(false)).unwrap(), false),
    ];

    inputs.iter().for_each(|(result, expected)| {
        assert_eq!(result, expected);
    });

    let failing_inputs = [
        (row1.evaluate_greater_than(1, &Decimal(5, 0))),
        (row1.evaluate_greater_than(0, &Bool(false))),
    ];

    failing_inputs.iter().for_each(|input| {
        assert!(matches!(input, Err(SqlError::ImpossibleComparison(_, _))))
    });
}

#[test]
fn evaluate_greater_than_equal() {
    use ColumnValue::*;

    let row1 = Row(vec![Int(5), Bool(true)]);
    let row2 = Row(vec![Int(6), Bool(false)]);

    let inputs = [
        (row1.evaluate_greater_than_equal(0, &Int(5)).unwrap(), true),
        (row1.evaluate_greater_than_equal(0, &Int(6)).unwrap(), false),
        (row1.evaluate_greater_than_equal(0, &Int(4)).unwrap(), true),
        (row1.evaluate_greater_than_equal(0, &Decimal(5, 0)).unwrap(), true),
        (row1.evaluate_greater_than_equal(1, &Bool(false)).unwrap(), true),
        (row1.evaluate_greater_than_equal(1, &Bool(true)).unwrap(), true),

        (row2.evaluate_greater_than_equal(1, &Bool(false)).unwrap(), true),
    ];

    inputs.iter().for_each(|(result, expected)| {
        assert_eq!(result, expected);
    });

    let failing_inputs = [
        (row1.evaluate_greater_than_equal(1, &Decimal(5, 0))),
        (row1.evaluate_greater_than_equal(0, &Bool(false))),
    ];

    failing_inputs.iter().for_each(|input| {
        assert!(matches!(input, Err(SqlError::ImpossibleComparison(_, _))))
    });
}

#[test]
fn select_with_where() {
    let (table, _) = test_table_with_values();

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
        test_row_set(vec![Row(vec![5.into()])]).unwrap()
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
        ]).unwrap()
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
