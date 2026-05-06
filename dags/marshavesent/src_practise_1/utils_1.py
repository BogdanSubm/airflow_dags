from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pendulum


API_URL = "https://b2b.itresume.ru/api/statistics"
BUCKET_NAME = 'marshavesent-bucket'

def get_week_boundaries(**context):
    """
    Вычисляет начало недели (Понедельник) и конец недели (Воскресенье) для даты выполнения.
    Заменяет логику diaily на weekly.
    """
    execution_date = pendulum.parse(context['ds'])
    
    # Понедельник текущей недели 
    week_start = execution_date.start_of('week')
    # Воскресенье текущей недели 
    week_end = execution_date.end_of('week')
    
    context['task_instance'].xcom_push(key='week_start', value=week_start.to_date_string())
    context['task_instance'].xcom_push(key='week_end', value=week_end.to_date_string())
    
    print(f"Границы недели: {week_start.to_date_string()} до {week_end.to_date_string()}")

def load_raw_data(**context):
    """
    Извлекает данные из API и сохраняет в таблицу сырых данных PostgreSQL.
    Удаляет существующие данные для периода перед вставкой новых.
    """
    import requests
    import psycopg2 as pg
    import ast
    
    # Параметры запроса
    ti = context['task_instance']
    week_start = ti.xcom_pull(task_ids='get_week_boundaries', key='week_start')
    week_end = ti.xcom_pull(task_ids='get_week_boundaries', key='week_end')
    
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': week_start,
        'end': week_end
    }
    
    print(f"Запрос данных из API для периода: {week_start} до {week_end}")
    response = requests.get(API_URL, params=payload)
    response.raise_for_status()
    data = response.json()
    
    print(f"Получено {len(data)} записей из API")
    
    connection = BaseHook.get_connection('conn_pg')
    
    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        
        # Идемпотичность
        delete_sql = """
            DELETE FROM marshavesent_raw_data 
            WHERE created_at >= %s::timestamp 
            AND created_at < %s::timestamp + interval '1 day'
        """
        cursor.execute(delete_sql, (week_start, week_end))
        print(f"Удалено существующие записи для периода {week_start} до {week_end}")
        
        # Вставка новых записей
        insert_sql = """
            INSERT INTO marshavesent_raw_data 
            (lti_user_id, is_correct, attempt_type, created_at, lesson_id, lesson_name, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        inserted_count = 0
        for el in data:
            row = []
            passback_params = ast.literal_eval(el.get('passback_params') if el.get('passback_params') else '{}')
            row.append(el.get('lti_user_id'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))
            row.append(passback_params.get('lesson_id'))
            row.append(passback_params.get('lesson_name'))
            
            # Достаем score если есть
            score = None
            if 'score' in passback_params:
                score = passback_params.get('score')
            elif el.get('score'):
                score = el.get('score')
            row.append(score)
            
            cursor.execute(insert_sql, row)
            inserted_count += 1
        
        conn.commit()
        print(f"Всего вставлено {inserted_count} записей в таблицу сырых данных")

def aggregate_data(**context):
    """
    Сводит данные из таблицы сырых данных и сохраняет в таблицу агрегированных данных.
    Вычисляет различные статистические данные по пользователям и типам попыток.
    """
    import psycopg2 as pg
    
    ti = context['task_instance']
    week_start = ti.xcom_pull(task_ids='get_week_boundaries', key='week_start')
    week_end = ti.xcom_pull(task_ids='get_week_boundaries', key='week_end')
    
    # Агрегация с метриками идемпотичность через Excluded
    aggregation_sql = """
        INSERT INTO marshavesent_agg_data
        SELECT 
            lti_user_id,
            attempt_type,
            lesson_id,
            lesson_name,
            COUNT(*) as attempt_count,
            COUNT(CASE WHEN is_correct THEN 1 END) as correct_count,
            COUNT(CASE WHEN NOT is_correct THEN 1 END) as incorrect_count,
            ROUND(AVG(score)::numeric, 2) as avg_score,
            MAX(score) as max_score,
            MIN(score) as min_score,
            ROUND(STDDEV(score)::numeric, 2) as stddev_score,
            MIN(created_at) as first_attempt,
            MAX(created_at) as last_attempt,
            %s::timestamp as week_start,
            %s::timestamp + interval '6 days' as week_end
        FROM marshavesent_raw_data
        WHERE created_at >= %s::timestamp 
            AND created_at < %s::timestamp + interval '1 day'
        GROUP BY lti_user_id, attempt_type, lesson_id, lesson_name
        ON CONFLICT (lti_user_id, attempt_type, lesson_id, week_start) 
        DO UPDATE SET
            attempt_count = EXCLUDED.attempt_count,
            correct_count = EXCLUDED.correct_count,
            incorrect_count = EXCLUDED.incorrect_count,
            avg_score = EXCLUDED.avg_score,
            max_score = EXCLUDED.max_score,
            min_score = EXCLUDED.min_score,
            stddev_score = EXCLUDED.stddev_score,
            first_attempt = EXCLUDED.first_attempt,
            last_attempt = EXCLUDED.last_attempt
    """
    
    connection = BaseHook.get_connection('conn_pg')
    
    with pg.connect(
        dbname='etl',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        sslmode='disable',
        keepalives_idle=600,
        tcp_user_timeout=600,
        connect_timeout=600
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(aggregation_sql, (week_start, week_start, week_start, week_end))
            conn.commit()
            print(f"Агрегированные данные для недели {week_start} до {week_end}")

def export_raw_to_csv(**context):
    """
    Экспортирует сырые данные в CSV и загружает в S3/Minio.
    Параллельно с экспортом агрегированных данных.
    """
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3
    from botocore.client import Config
    import codecs
    
    ti = context['task_instance']
    week_start = ti.xcom_pull(task_ids='get_week_boundaries', key='week_start')
    week_end = ti.xcom_pull(task_ids='get_week_boundaries', key='week_end')
    
    # Query raw data for the week
    sql_query = """
        SELECT lti_user_id, attempt_type, is_correct, created_at, lesson_id, lesson_name, score
        FROM marshavesent_raw_data
        WHERE created_at >= %s::timestamp 
            AND created_at < %s::timestamp + interval '1 day'
        ORDER BY created_at
    """
    
    conn = BaseHook.get_connection('conn_pg')
    
    with pg.connect(
        dbname='etl',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        sslmode='disable',
        keepalives_idle=600,
        tcp_user_timeout=600,
        connect_timeout=600
    ) as pg_conn:
        cursor = pg_conn.cursor()
        cursor.execute(sql_query, (week_start, week_end))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"Получено {len(rows)} записей для экспорта в CSV")
    
    # Создание CSV в памяти
    file = BytesIO()
    writer_wrapper = codecs.getwriter('utf-8')
    
    writer = csv.writer(
        writer_wrapper(file),
        delimiter=',',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    
    # Запись заголовка
    writer.writerow(columns)
    # Запись данных
    writer.writerows(rows)
    
    file.seek(0)
    
    # Загрузка в S3/Minio
    connection_s3 = BaseHook.get_connection('conn_s3')
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=connection_s3.login,
        aws_secret_access_key=connection_s3.password,
        endpoint_url=connection_s3.host,
        config=Config(signature_version='s3v4', connect_timeout=600, read_timeout=600)
    )
    
    key = f'marshavesent_raw_{week_start}_to_{week_end}.csv'
    s3_client.put_object(
        Body=file,
        Bucket=BUCKET_NAME,
        Key=key
    )
    file.close()
    print(f"Сырые данные экспортированы в {key}")

def export_aggregated_to_csv(**context):
    """
    Экспортирует агрегированные данные в CSV и загружает в S3/Minio.
    """
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3
    from botocore.client import Config
    import codecs
    
    ti = context['task_instance']
    week_start = ti.xcom_pull(task_ids='get_week_boundaries', key='week_start')
    week_end = ti.xcom_pull(task_ids='get_week_boundaries', key='week_end')
    
    # Query aggregated data for the week
    sql_query = """
        SELECT 
            lti_user_id,
            attempt_type,
            lesson_id,
            lesson_name,
            attempt_count,
            correct_count,
            incorrect_count,
            avg_score,
            max_score,
            min_score,
            stddev_score,
            first_attempt,
            last_attempt,
            week_start,
            week_end
        FROM marshavesent_agg_data
        WHERE week_start = %s::timestamp
        ORDER BY lti_user_id, attempt_type
    """
    
    conn = BaseHook.get_connection('conn_pg')
    
    with pg.connect(
        dbname='etl',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        sslmode='disable',
        keepalives_idle=600,
        tcp_user_timeout=600,
        connect_timeout=600
    ) as pg_conn:
        cursor = pg_conn.cursor()
        cursor.execute(sql_query, (week_start,))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"Получено {len(rows)} записей для экспорта в CSV")
    
    # Создание CSV в памяти
    file = BytesIO()
    writer_wrapper = codecs.getwriter('utf-8')
    
    writer = csv.writer(
        writer_wrapper(file),
        delimiter=',',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    
    # Запись заголовка
    writer.writerow(columns)
    # Запись данных
    writer.writerows(rows)
    
    file.seek(0)
    
    # Загрузка в S3/Minio
    connection_s3 = BaseHook.get_connection('conn_s3')
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=connection_s3.login,
        aws_secret_access_key=connection_s3.password,
        endpoint_url=connection_s3.host,
        config=Config(signature_version='s3v4', connect_timeout=600, read_timeout=600)
    )
    
    key = f'marshavesent_agg_{week_start}_to_{week_end}.csv'
    s3_client.put_object(
        Body=file,
        Bucket=BUCKET_NAME,
        Key=key
    )
    file.close()
    print(f"Агрегированные данные экспортированы в {key}")
