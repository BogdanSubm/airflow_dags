from airflow.hooks.base import BaseHook

import psycopg2 as pg


def _get_pg_conn(conn_id='conn_pg', dbname='etl'):
    connection = BaseHook.get_connection(conn_id)
    return pg.connect(
        dbname=dbname,
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600,
    )