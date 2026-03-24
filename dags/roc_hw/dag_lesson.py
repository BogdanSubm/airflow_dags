from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from roc_hw.plugins.operators import PgOperator
from roc_hw.plugins.sensors import CheckTablesSensor

from airflow.sensors.external_task import ExternalTaskSensor
import roc_hw.plugins.utils_lesson as myutils
import roc_hw.plugins.templates as mytemplates
from roc_hw.plugins import sqls


DEFAULT_ARGS = {
    'owner': 'roc',
    'retries': 2,
    'retries_delay': 600,
    'start_date': datetime(2026, 2, 16)
}

API_URL = 'https://b2b.itresume.ru/api/statistics'

with DAG(
    dag_id='roc_api_to_pg_agg_s3',
    schedule='0 0 * * 1',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={'date_tag': mytemplates.make_date_tag},
    render_template_as_native_obj=True,
    tags=['roc'],
    params={'api_url': API_URL},
    doc_md="Получаем сырые данные по api, кладем в pg (и сразу в s3), добавляем новых пользователей в таблицу users > данные агрегируем и снова pg+s3"
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=myutils.load_from_api,
        op_kwargs={
            'API_URL': API_URL,
            'date_tag': '{{date_tag(data_interval_start)}}'},
    )

    aggregate_data_1 = PgOperator(
        task_id='aggregate_data_1',
        conn_id='conn_pg',
        sql_query=sqls.sql_agg,
        sql_query_parameters=("{{data_interval_start}}",
                              "{{data_interval_end}}",
                              "{{data_interval_start}}",
                              "{{data_interval_start}}",
                              "{{data_interval_end}}",
                              )
    )

    add_users = PgOperator(
        task_id='add_users',
        conn_id='conn_pg',
        sql_query=sqls.sql_add_users,
        sql_query_parameters=("{{data_interval_start}}",
                              "{{data_interval_end}}",
                              )
    )

    check_tables = CheckTablesSensor(
        task_id='check_tables',
        mode='reschedule',
        poke_interval=300,
        conn_id='conn_pg',
        tables_to_check=('roc_raw_data', 'roc_users')
    )

    # check_bash_opp = ExternalTaskSensor(
    #     task_id='check_bash_opp',
    #     mode='reschedule',
    #     poke_interval=300,
    #     external_dag_id='roc_lib_show',
    #     external_task_id='lib_test',
    #     execution_delta=timedelta(hours=3)
    # )

    upload_agg_data_s3 = PythonOperator(
        task_id='upload_agg_data_s3',
        python_callable=myutils.upload_agg_data_s3
    )

    upload_raw_data_s3 = PythonOperator(
        task_id='upload_raw_data_s3',
        python_callable=myutils.upload_raw_data_s3
    )

    # dag_start >> load_from_api >> add_users >> check_tables >> aggregate_data_1 >> dag_end
    dag_start >> load_from_api >> add_users >> check_tables >> aggregate_data_1 >> upload_agg_data_s3 >> dag_end
    check_tables >> upload_raw_data_s3 >> dag_end
