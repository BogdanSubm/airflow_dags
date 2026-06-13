# -*- coding: utf-8 -*-
# Всё, что связано с базой данных: подключение, создание таблицы, заливка данных

import psycopg2 as pg
from airflow.hooks.base import BaseHook

def get_pg_conn():
    """Одно подключение на всех — не плодим лишний код"""
    conn = BaseHook.get_connection('conn_pg')
    return pg.connect(
        dbname='etl',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        sslmode='disable',
        connect_timeout=600,
    )

def create_table(table_name: str, ddl_sql: str, **context):
    """Создаём таблицу, если её нет (идемпотентно)"""
    log = context['ti'].log
    log.info(f'📦 Создаю таблицу {table_name}')
    
    ddl_with_name = ddl_sql.format(table=table_name)
    
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_with_name)
        conn.commit()
    log.info(f'✅ Таблица {table_name} готова')

def load_table(table_name: str, dml_sql: str, **context):
    """
    Загружаем данные за текущий день.
    Фишка: удаляем только данные за текущую дату (не трогаем историю)
    """
    log = context['ti'].log
    ds = context['ds']  # дата запуска DAG
    log.info(f'💾 Загружаю данные в {table_name} за {ds}')
    
    # Подставляем имя таблицы в запрос
    dml_with_name = dml_sql.format(table=table_name)
    
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # Чистим только данные за сегодня (чтобы не было дублей)
            cur.execute(f"DELETE FROM {table_name} WHERE load_date = %s", (ds,))
            # Вставляем свежие данные
            cur.execute(dml_with_name)
        conn.commit()
    
    # Проверяем, что данные реально появились
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE load_date = %s", (ds,))
            count = cur.fetchone()[0]
    
    if count == 0:
        raise ValueError(f"❌ Нет данных для {table_name} за {ds}")
    
    log.info(f'✅ Загружено {count} строк в {table_name}')
