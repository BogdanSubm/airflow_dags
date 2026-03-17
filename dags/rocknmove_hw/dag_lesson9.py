from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import rocknmove_hw.plugins.utils_lesson9 as myutils
import rocknmove_hw.plugins.templates as mytemplates


DEFAULT_ARGS = {
    'owner': 'rocknmove',
    'retries': 2,
    'retries_delay': 600,
    'start_date': datetime(2026, 2, 16),
}

API_URL = 'https://b2b.itresume.ru/api/statistics'

with DAG(
    dag_id='rocknmove_api_to_pg_agg_s3',
    schedule='0 0 * * 1',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={'date_tag': mytemplates.make_date_tag},
    render_template_as_native_obj=True,
    tags=['rocknmove'],
    doc_md="Получаем сырые данные по api, кладем в pg (и сразу в s3), добавляем новых пользователей в таблицу users > данные агрегируем и снова pg+s3"
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    check_tables = PythonOperator(
        task_id='check_tables',
        python_callable=myutils.check_tables
    )

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=myutils.load_from_api,
        op_kwargs={
            'API_URL': API_URL,
            'date_tag': '{{date_tag(data_interval_start)}}'},
    )

    aggregate_data_1 = PythonOperator(
        task_id='aggregate_data_1',
        python_callable=myutils.aggregate_data_1,
        op_kwargs={'API_URL': API_URL},
    )

    add_users = PythonOperator(
        task_id='add_users',
        python_callable=myutils.add_users
    )

    upload_agg_data_s3 = PythonOperator(
        task_id='upload_agg_data_s3',
        python_callable=myutils.upload_agg_data_s3
    )

    upload_raw_data_s3 = PythonOperator(
        task_id='upload_raw_data_s3',
        python_callable=myutils.upload_raw_data_s3
    )

    # dag_start >> check_tables >> load_from_api >> aggregate_data_1 >> add_users >> dag_end
    dag_start >> check_tables >> load_from_api >> aggregate_data_1 >> add_users >> upload_agg_data_s3 >> dag_end
    load_from_api >> upload_raw_data_s3 >> dag_end
