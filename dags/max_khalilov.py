from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Параметры по умолчанию (константы)
DEFAULT_ARGS = {
    'owner': 'admin', # Владелец DAG
    'retries': 2, # Кол-во повторов
    'retry_delay': 600, # Задержка перед повтором
    'start_date': datetime(2025, 4, 11), # Дата начала выполнения
}

API_URL = "https://b2b.itresume.ru/api/statistics"

def load_from_api(**context):
    import requests # Запросы к API
    import pendulum # Работа с датами (прокаченный datetime)
    import psycopg2 as pg # библиотека для работы с PostgreSQL
    import ast # Преобразование строк в словари

    payload = {
        'client':'Skillfactory', # Название клиента
        'client_key': 'M2MGWS', # Ключ клиента
        'start': context['ds'], # Дата начала. ds - логическая дата 
        'end': pendulum.parse(context['ds']).add(days=1).to_date_string(), # Дата конца. Обращаемся к pendulum, преобразуем ds в дату, добавляем 1 день и преобразуем в строку
    }
    response = requests.get(API_URL, params=payload) # Отправляем запрос на API
    data = response.json() # Преобразуем ответ в словарь
    
    connection = BaseHook.get_connection('conn_pg') # Получаем соединение с базой данных    

    with pg.connect(
        dbname='etl', # Название базы данных
        sslmode='disable', # Режим шифрования
        user=connection.login, # Логин
        password=connection.password, # Пароль
        host=connection.host, # Хост
        port=connection.port, # Порт
        connect_timeout=600, # Таймаут соединения
        keepalives=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor() # Создаём курсор

        for el in data: # Проходим по всем элементам словаря
            row = [] # Создаём список для строки
            passback_params = ast.literal_eval(el.get('passback_params', '{}')) # Преобразуем строку в словарь
            row.append(el.get('lti_user_id')) # Добавляем в список значение из словаря
            row.append(True if el.get('is_correct') == 1 else False) # Добавляем в список значение из словаря
            row.append(el.get('attempt_type')) # Добавляем в список значение из словаря
            row.append(el.get('created_at')) # Добавляем в список значение из словаря
            row.append(passback_params.get('oauth_consumer_key')) # Добавляем в список значение из словаря
            row.append(passback_params.get('lis_result_sourcedid')) # Добавляем в список значение из словаря
            row.append(passback_params.get('lis_outcome_service_url')) # Добавляем в список значение из словаря

            cursor.execute("INSERT INTO new_table VALUES (%s, %s, %s, %s, %s, %s, %s)", row) # Выполняем запрос

        conn.commit() # Сохраняем изменения


# Создание DAG
with DAG(
    dag_id="max_khalilov_dag", # Изменено с email на допустимый формат
    tags=['7', 'max'], # Теги. По ним можно так же находить DAG
    schedule='@daily', # Расписание выполнения
    default_args=DEFAULT_ARGS, # Параметры по умолчанию
    max_active_runs=1, # Кол-во одновременных выполнений
    max_active_tasks=1 # Кол-во одновременных задач
) as dag:
    
    dag_start = EmptyOperator(task_id='start_dag')
    dag_end = EmptyOperator(task_id='end_dag')

    # Загрузка данных из API
    load_from_api = PythonOperator(
        task_id='load_from_api', # Идентификатор задачи
        python_callable=load_from_api # Функция для выполнения
    )

    dag_start >> load_from_api >> dag_end 