config = [
    { # расчет суммы
        'table_name': 'roc_test_agg1_spiridonov',
        'table_ddl': 'create table if not exists {table} (category text, amount int, load_date date)',
        'table_dml': '''
            insert into {table} 
            select category, sum(amount), '{{ ds }}'::date as load_date
            from roc_test_raw_data 
            group by category
        ''',
        'need_to_export': False,
    },
    { # расчет среднего значения
        'table_name': 'roc_test_agg2_spiridonov',
        'table_ddl': 'create table if not exists {table} (category text, amount int, load_date date)',
        'table_dml': '''
            insert into {table} 
            select category, avg(amount), '{{ ds }}'::date as load_date
            from roc_test_raw_data 
            group by category
        ''',
        'need_to_export': True,
    },
]