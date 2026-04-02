config = [
    {
        'table_name': 'roc_test_agg1',
        'table_ddl': 'create table if not exists {} (category text, amount int, load_date date)',
        'table_dml': '''
            insert into {} 
            select category, sum(amount), '{{ ds }}'::date as load_date
            from roc_test_raw_data 
            group by category
        ''',
        'need_to_export': False,
    },
    {
        'table_name': 'roc_test_agg2',
        'table_ddl': 'create table if not exists {} (category text, amount int, load_date date)',
        'table_dml': '''
            insert into {} 
            select category, sum(amount)/count(1), '{{ ds }}'::date as load_date
            from roc_test_raw_data 
            group by category
        ''',
        'need_to_export': True,
    },
]