from __future__ import annotations

import csv
import logging
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ftp.hooks.ftp import FTPHook

log = logging.getLogger(__name__)

PG_CONN_ID = "conn_pg"
FTP_CONN_ID = "conn_si"

RAW_TABLE = "raw_orders_cafe_gms"
BRANCHES = ["Центр", "Север"]
# (блюдо, категория, цена)
MENU = [
    ("Пицца", "Основные блюда", 650.00),
    ("Паста", "Основные блюда", 480.00),
    ("Кофе", "Напитки", 220.00),
    ("Чизкейк", "Десерты", 390.00),
]

# Конфиг агрегатов 
CONFIG = [
    {
        "table_name": "cafe_revenue_by_branch_gms",
        "need_to_export": True,
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS cafe_revenue_by_branch_gms (
                report_date DATE,
                branch TEXT,
                orders_count INT,
                total_revenue NUMERIC(14,2)
            );
        """,
        "table_dml": """
            INSERT INTO cafe_revenue_by_branch_gms
                (report_date, branch, orders_count, total_revenue)
            SELECT report_date, branch, COUNT(*), SUM(amount)
            FROM raw_orders_cafe_gms
            WHERE report_date = %(ds)s
            GROUP BY report_date, branch;
        """,
    },
    {
        "table_name": "cafe_top_dishes_gms",
        "need_to_export": True,
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS cafe_top_dishes_gms (
                report_date DATE,
                dish TEXT,
                total_qty INT,
                total_revenue NUMERIC(14,2)
            );
        """,
        "table_dml": """
            INSERT INTO cafe_top_dishes_gms
                (report_date, dish, total_qty, total_revenue)
            SELECT report_date, dish, SUM(qty), SUM(amount)
            FROM raw_orders_cafe_gms
            WHERE report_date = %(ds)s
            GROUP BY report_date, dish
            ORDER BY SUM(amount) DESC;
        """,
    },
    {
        "table_name": "cafe_stats_by_category_gms",
        "need_to_export": False,
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS cafe_stats_by_category_gms (
                report_date DATE,
                category TEXT,
                total_qty INT,
                total_revenue NUMERIC(14,2),
                avg_price NUMERIC(10,2)
            );
        """,
        "table_dml": """
            INSERT INTO cafe_stats_by_category_gms
                (report_date, category, total_qty, total_revenue, avg_price)
            SELECT report_date, category, SUM(qty), SUM(amount), AVG(price)
            FROM raw_orders_cafe_gms
            WHERE report_date = %(ds)s
            GROUP BY report_date, category;
        """,
    },
]

# наполнение raw-таблицы 
def seed_raw_orders(**context):
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            id SERIAL PRIMARY KEY,
            report_date DATE NOT NULL,
            branch TEXT NOT NULL,
            dish TEXT NOT NULL,
            category TEXT NOT NULL,
            qty INT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            amount NUMERIC(12,2) NOT NULL
        );
    """)

    # чистим данные за эту дату перед вставкой
    hook.run(f"DELETE FROM {RAW_TABLE} WHERE report_date = %(ds)s",
             parameters={"ds": ds})

    # детерминированные синтетические данные (одинаковы при перезапуске за ту же дату)
    rnd = random.Random(ds)
    rows = []
    for branch in BRANCHES:
        for dish, category, price in MENU:
            qty = rnd.randint(5, 40)
            rows.append((ds, branch, dish, category, qty, price, round(qty * price, 2)))

    hook.insert_rows(
        table=RAW_TABLE,
        rows=rows,
        target_fields=["report_date", "branch", "dish", "category", "qty", "price", "amount"],
    )
    log.info("seed: вставлено %s строк за %s", len(rows), ds)


# Создание агрегатной таблицы
def create_table(table_ddl, **context):
    PostgresHook(postgres_conn_id=PG_CONN_ID).run(table_ddl)


# Заполнение таблицы
def fill_table(table_name, table_dml, **context):
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(f"DELETE FROM {table_name} WHERE report_date = %(ds)s",
             parameters={"ds": ds})
    hook.run(table_dml, parameters={"ds": ds})
    log.info("fill: %s обновлена за %s", table_name, ds)


# Экспорт в хранилище (FTP conn_si)
def export_to_storage(table_name, **context):
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} WHERE report_date = %(ds)s", {"ds": ds})
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    cur.close()
    conn.close()

    local_path = f"/tmp/{table_name}_{ds}.csv"
    with open(local_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    log.info("export: CSV собран (%s строк) -> %s", len(rows), local_path)

    remote_name = f"{table_name}_{ds}.csv"
    try:
        ftp = FTPHook(ftp_conn_id=FTP_CONN_ID)
        ftp.store_file(remote_name, local_path)
        log.info("export: загружено на FTP (conn_si): %s", remote_name)
    except Exception as e:
        log.warning("export: FTP недоступен (%s). Файл оставлен локально: %s", e, local_path)


# Dag
default_args = {
    "owner": "goldmsara",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="cafe_dynamic_agg_goldmsara",
    default_args=default_args,
    description="Динамическая генерация сводных таблиц сети кафе",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 15),
    end_date=datetime(2024, 1, 17),
    catchup=True,
    max_active_runs=1,
    tags=["cafe", "goldmsara"],
) as dag:

    seed = PythonOperator(
        task_id="seed_raw_orders",
        python_callable=seed_raw_orders,
    )

    for cfg in CONFIG:
        tname = cfg["table_name"]

        t_create = PythonOperator(
            task_id=f"create__{tname}",
            python_callable=create_table,
            op_kwargs={"table_ddl": cfg["table_ddl"]},
        )

        t_fill = PythonOperator(
            task_id=f"fill__{tname}",
            python_callable=fill_table,
            op_kwargs={"table_name": tname, "table_dml": cfg["table_dml"]},
        )

        seed >> t_create >> t_fill

        if cfg["need_to_export"]:
            t_export = PythonOperator(
                task_id=f"export__{tname}",
                python_callable=export_to_storage,
                op_kwargs={"table_name": tname},
            )
            t_fill >> t_export