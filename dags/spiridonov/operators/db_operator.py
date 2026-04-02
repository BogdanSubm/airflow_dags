from airflow.operators.python import get_current_context

from utils.db import get_pg_connection

def create_table(table_name, ddl):
    context = get_current_context()
    log = context['ti'].log
    log.info(f'Start creating table {table_name}')
    # подставляем имя таблицы в ddl
    ddl_query = ddl.format(table=table_name)

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            # выполняем ddl запрос
            cur.execute(ddl_query)
        conn.commit()
    log.info(f'End creating table {table_name}')

def load_table(table_name, dml, ds, **kwargs):
    context = get_current_context()
    log = context['ti'].log
    ds = context['ds']
    log.info(f'Start loading table {table_name}, date: {ds}')
    # подставляем имя таблицы в dml
    dml_query = dml.format(table=table_name)

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            log.info('Cleaning previous data')
            # соблюдаем идемпотентность
            cur.execute(f"""
                DELETE FROM {table_name}
                WHERE load_date = %s
            """, (ds,))
            cur.execute(dml_query)
            # вставляем данные
            log.info('Executing DML')
            cur.execute(dml_query)
            cur.execute(f"""
                SELECT COUNT(1) 
                FROM {table_name}
                WHERE load_date = %s
            """, (ds,))
            # количество строк
            count = cur.fetchone()[0]
            log.info(f'Total rows: {count}')
            # выбрасываем исключение если данных нет
            if count == 0:
                raise ValueError(f'No data loaded into {table_name}')
        conn.commit()
    log.info(f'End loading table {table_name}')