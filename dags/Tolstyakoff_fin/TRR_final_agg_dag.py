# -*- coding: utf-8 -*-
# Финальный DAG для динамического создания агрегатов
# Добавляем цвета в Graph View для красоты

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import sys
from pathlib import Path

# Добавляем текущую папку в пути, чтобы видеть наши модули
sys.path.append(str(Path(__file__).parent))

from TRR_agg_config import config
from TRR_db_utils import create_table, load_table
from TRR_s3_uploader import export_to_s3

# Стандартные настройки DAG
DEFAULT_ARGS = {
    'owner': 'tolstyakoff',
    'retries': 2,
    'retry_delay': 600,      # 10 минут между повторами
    'start_date': datetime(2026, 6, 5),
}

with DAG(
    dag_id='TRR_final_agg_dag',
    tags=['final', 'tolstyakoff', 'dynamic'],
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    catchup=True,            # Не догоняем пропущенные дни
    max_active_runs=1,       # Только один запуск DAG одновременно
    description='Динамический агрегатор таблиц по конфигу',
) as dag:

    # Цветные заглушки для красивого графа
    start = EmptyOperator(
        task_id='▶️_start',
        ui_color='#2E8B57',   # морская волна
        ui_fgcolor='#FFFFFF'  # белый текст
    )
    
    end = EmptyOperator(
        task_id='⏹️_end',
        ui_color='#8B0000',   # тёмно-красный
        ui_fgcolor='#FFFFFF'
    )
    
    # Список для хранения групп задач (чтобы красиво связать их в конце)
    task_groups = []
    
    # Основной цикл: для каждой таблицы из конфига создаём свой пайплайн
    for cfg in config:
        table_name = cfg['table_name']
        
        # Каждая таблица — отдельная группа задач (TaskGroup)
        with TaskGroup(group_id=f'{table_name}_pipeline') as tg:
            
            # 1. Создаём таблицу (если нет)
            create = PythonOperator(
                task_id=f'create_{table_name}',
                python_callable=create_table,
                op_kwargs={'table_name': table_name, 'ddl_sql': cfg['table_ddl']},
                # Цвета для оператора (не все версии Airflow поддерживают)
                # ui_color='#4682B4',  # стальной синий
            )
            
            # 2. Загружаем данные
            load = PythonOperator(
                task_id=f'load_{table_name}',
                python_callable=load_table,
                op_kwargs={'table_name': table_name, 'dml_sql': cfg['table_dml']},
            )
            
            create >> load  # сначала создай, потом загружай
            
            # 3. Если в конфиге флаг экспорта — добавляем задачу выгрузки в S3
            if cfg.get('need_to_export', False):  # по умолчанию False
                export = PythonOperator(
                    task_id=f'export_{table_name}_to_s3',
                    python_callable=export_to_s3,
                    op_kwargs={'table_name': table_name},
                    retries=3,   # у экспорта больше шансов на успех
                )
                load >> export
                last_task = export
            else:
                last_task = load
        
        # Запоминаем последнюю задачу группы для связки с end
        task_groups.append(tg)
    
    # Собираем пайплайн: старт → все группы параллельно → конец
    start >> task_groups >> end
