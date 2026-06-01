config = [
    {
        'table_name': 'f0rtunaa_agg_sales_by_category',
        'table_ddl': 'create table if not exists {table_name} (category text, amount int, load_date date)',
        'table_dml': '''
            insert into {table_name}
            select category, sum(amount), '{{ ds }}'::date as load_date
            from f0rtunaa_raw_sales
            group by category
        ''',
        'need_to_export': False,
    },
    {
        'table_name': 'f0rtunaa_agg_sales_by_product',
        'table_ddl': 'create table if not exists {table_name} (product text, amount int, load_date date)',
        'table_dml': '''
            insert into {table_name}
            select product, sum(amount), '{{ ds }}'::date as load_date
            from f0rtunaa_raw_sales
            group by product
        ''',
        'need_to_export': True,
    },
]