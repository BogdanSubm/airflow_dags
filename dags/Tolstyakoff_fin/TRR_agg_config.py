# Реальный конфиг для 'эксперимента'

config = [
    {
        # Агрегат 1: средняя цена по категориям
        'table_name': 'tolstyakoff_test_agg_prices',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS {table} (
                category_name TEXT,
                avg_price NUMERIC(10,2),
                load_date DATE
            )
        """,
        'table_dml': """
            INSERT INTO {table}
            SELECT 
                category_name,
                AVG(price) as avg_price,
                '{{ ds }}'::DATE as load_date
            FROM tolstyakoff_test_raw_sales
            WHERE sale_date >= '{{ ds }}'::DATE
            GROUP BY category_name
        """,
        'need_to_export': True,   # выгрузим в S3 для проверки
    },
    {
        # Агрегат 2: количество продаж по менеджерам
        'table_name': 'tolstyakoff_test_agg_managers',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS {table} (
                manager_name TEXT,
                total_sales INTEGER,
                load_date DATE
            )
        """,
        'table_dml': """
            INSERT INTO {table}
            SELECT 
                manager_name,
                COUNT(*) as total_sales,
                '{{ ds }}'::DATE as load_date
            FROM tolstyakoff_test_raw_sales
            WHERE sale_date >= '{{ ds }}'::DATE
            GROUP BY manager_name
        """,
        'need_to_export': False,  # только в БД
    },
]
