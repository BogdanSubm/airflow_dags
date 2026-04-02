from lib2to3.fixes.fix_input import context

from utils.db import get_pg_connection

def create_table(table_name, ddl, **context):
    log = context['ti'].log
    log.info(f'Start creating table {table_name}')
    ddl_query = ddl.format(table_name)

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_query)
        conn.commit()
    log.info(f'End creating table {table_name}')

def load_table(table_name, dml, ds, **context):
    log = context['ti'].log
    log.info(f'Start loading table {table_name}, date: {ds}')
    dml_query = dml.format(table_name)

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            log.info('Cleaning previous data')
            cur.execute(f"""
                DELETE FROM {table_name}
                WHERE load_date = '{ds}'
            """)
            log.info('Executing DML')
            cur.execute(dml_query)

            cur.execute(f"""
                SELECT COUNT(1) 
                FROM {table_name}
                WHERE load_date = '{ds}'
            """)
            count = cur.fetchone()[0]
            log.info(f'Total rows: {count}')
            if count == 0:
                raise ValueError(f'No data loaded into {table_name}')
        conn.commit()
    log.info(f'End loading table {table_name}')