config = [
    {
        'table_name': 'roc_test_agg1',
        'table_ddl': 'create table if not exists {} (category text, amount int)',
        'table_dml': 'select category, sum(amount) as sum_amount from roc_test_raw_data group by category',
        'need_to_export': False,
    },
    {
        'table_name': 'roc_test_agg2',
        'table_ddl': 'create table if not exists {} (category text, amount int)',
        'table_dml': 'select category, sum(amount)/count(1) as average_amount from roc_test_raw_data group by category',
        'need_to_export': True,
    },
]
