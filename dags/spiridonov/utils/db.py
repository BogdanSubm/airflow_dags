import psycopg2 as pg
from airflow.hooks.base import BaseHook

def get_pg_connection():
    conn = BaseHook.get_connection('conn_pg')

    return pg.connect(
        dbname='etl2',
        sslmode='disable',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )