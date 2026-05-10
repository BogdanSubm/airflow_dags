from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pendulum

API_URL = "https://b2b.itresume.ru/api/statistics"
BUCKET_NAME = 'marshavesent-bucket'

# ============================================
# SQL КОНСТАНТЫ (используются в DAG и операторах)
# ============================================

CREATE_RAW_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS marshavesent_raw_data (
        id SERIAL PRIMARY KEY,
        lti_user_id VARCHAR(255),
        is_correct BOOLEAN,
        attempt_type VARCHAR(50),
        created_at TIMESTAMP,
        lesson_id VARCHAR(255),
        lesson_name VARCHAR(500),
        score DECIMAL(10,2)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_created_at 
    ON marshavesent_raw_data(created_at);
"""

CREATE_AGG_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS marshavesent_agg_data (
        id SERIAL PRIMARY KEY,
        lti_user_id VARCHAR(255),
        attempt_type VARCHAR(50),
        lesson_id VARCHAR(255),
        lesson_name VARCHAR(500),
        attempt_count INTEGER,
        correct_count INTEGER,
        incorrect_count INTEGER,
        avg_score DECIMAL(10,2),
        max_score DECIMAL(10,2),
        min_score DECIMAL(10,2),
        stddev_score DECIMAL(10,2),
        first_attempt TIMESTAMP,
        last_attempt TIMESTAMP,
        week_start TIMESTAMP,
        week_end TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(lti_user_id, attempt_type, lesson_id, week_start)
    )
"""

CREATE_MONTHLY_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS marshavesent_raw_data_monthly (
        id SERIAL PRIMARY KEY,
        lti_user_id VARCHAR(255),
        is_correct BOOLEAN,
        attempt_type VARCHAR(50),
        created_at TIMESTAMP,
        lesson_id VARCHAR(255),
        lesson_name VARCHAR(500),
        score DECIMAL(10,2)
    );
    CREATE INDEX IF NOT EXISTS idx_monthly_created_at 
    ON marshavesent_raw_data_monthly(created_at);
"""

AGGREGATION_SQL = """
    INSERT INTO marshavesent_agg_data 
    (lti_user_id, attempt_type, lesson_id, lesson_name, 
     attempt_count, correct_count, incorrect_count, 
     avg_score, max_score, min_score, stddev_score,
     first_attempt, last_attempt, week_start, week_end)
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
        %(week_start)s::timestamp as week_start,
        %(week_start)s::timestamp + interval '6 days' as week_end
    FROM marshavesent_raw_data
    WHERE created_at >= %(week_start)s::timestamp 
        AND created_at < %(week_end)s::timestamp + interval '1 day'
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

CHECK_WEEKLY_DATA_SQL = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT lti_user_id) as unique_users,
        MIN(created_at) as first_record,
        MAX(created_at) as last_record
    FROM marshavesent_raw_data
    WHERE created_at >= %(week_start)s::timestamp 
        AND created_at < %(week_end)s::timestamp + interval '1 day';
"""

CHECK_MONTHLY_DATA_SQL = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT lti_user_id) as unique_users,
        MIN(created_at) as first_record,
        MAX(created_at) as last_record
    FROM marshavesent_raw_data_monthly
    WHERE created_at >= %(month_start)s::timestamp 
        AND created_at < %(month_end)s::timestamp + interval '1 day';
"""

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================

def _get_db_connection(conn_id='conn_pg', database='etl'):
    """Создает подключение к PostgreSQL."""
    import psycopg2 as pg
    connection = BaseHook.get_connection(conn_id)
    return pg.connect(
        dbname=database,
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    )

def _parse_passback_params(passback_str):
    """Безопасно парсит passback_params."""
    import ast
    if not passback_str:
        return {}
    try:
        return ast.literal_eval(passback_str)
    except (ValueError, SyntaxError):
        return {}

def _upload_to_s3(file_obj, key, bucket=BUCKET_NAME):
    """Загружает файл в S3/Minio."""
    import boto3
    from botocore.client import Config
    
    connection_s3 = BaseHook.get_connection('conn_s3')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=connection_s3.login,
        aws_secret_access_key=connection_s3.password,
        endpoint_url=connection_s3.host,
        config=Config(signature_version='s3v4', connect_timeout=600, read_timeout=600)
    )
    s3_client.put_object(Body=file_obj, Bucket=bucket, Key=key)
    print(f"Файл загружен в {bucket}/{key}")

# ============================================
# ФУНКЦИИ ДЛЯ ЕЖЕНЕДЕЛЬНОЙ ЗАГРУЗКИ
# ============================================

def load_raw_data(week_start=None, week_end=None, **context):
    """
    Загружает сырые данные за неделю из API в PostgreSQL.
    Параметры получает через op_kwargs (поддерживает Jinja).
    """
    import requests
    import psycopg2 as pg
    import ast
    
    # Получаем параметры (приоритет: op_kwargs > XCom)
    if week_start is None or week_end is None:
        ti = context.get('task_instance')
        if ti:
            week_start = ti.xcom_pull(task_ids='get_week_boundaries', key='week_start')
            week_end = ti.xcom_pull(task_ids='get_week_boundaries', key='week_end')
    
    if not week_start or not week_end:
        raise ValueError(f"Не указаны week_start={week_start} или week_end={week_end}")
    
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': week_start,
        'end': week_end
    }
    
    print(f"Запрос данных из API: {week_start} до {week_end}")
    response = requests.get(API_URL, params=payload, timeout=60)
    response.raise_for_status()
    data = response.json()
    print(f"Получено {len(data)} записей из API")
    
    if not data:
        print("ПРЕДУПРЕЖДЕНИЕ: API вернул пустой список данных!")
        return
    
    with _get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Создаем таблицу если нужно
        cursor.execute(CREATE_RAW_TABLE_SQL)
        
        # Идемпотентность: удаляем старые данные за период
        cursor.execute(
            """DELETE FROM marshavesent_raw_data 
               WHERE created_at >= %s::timestamp 
               AND created_at < %s::timestamp + interval '1 day'""",
            (week_start, week_end)
        )
        print(f"Очищены данные за {week_start} - {week_end}")
        
        # Вставка новых данных
        insert_sql = """
            INSERT INTO marshavesent_raw_data 
            (lti_user_id, is_correct, attempt_type, created_at, lesson_id, lesson_name, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        inserted = 0
        for el in data:
            pp = _parse_passback_params(el.get('passback_params'))
            cursor.execute(insert_sql, (
                el.get('lti_user_id'),
                True if el.get('is_correct') == 1 else False,
                el.get('attempt_type'),
                el.get('created_at'),
                pp.get('lesson_id'),
                pp.get('lesson_name'),
                pp.get('score') or el.get('score')
            ))
            inserted += 1
        
        conn.commit()
        print(f"Вставлено {inserted} записей")

def export_raw_to_csv(week_start=None, week_end=None, **context):
    """Экспортирует сырые недельные данные в CSV."""
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import codecs
    
    if week_start is None or week_end is None:
        ti = context.get('task_instance')
        if ti:
            week_start = ti.xcom_pull(task_ids='get_week_boundaries', key='week_start')
            week_end = ti.xcom_pull(task_ids='get_week_boundaries', key='week_end')
    
    with _get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT lti_user_id, attempt_type, is_correct, created_at, lesson_id, lesson_name, score
               FROM marshavesent_raw_data
               WHERE created_at >= %s::timestamp AND created_at < %s::timestamp + interval '1 day'
               ORDER BY created_at""",
            (week_start, week_end)
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"Выгружено {len(rows)} строк для CSV")
    
    file = BytesIO()
    writer = csv.writer(codecs.getwriter('utf-8')(file))
    writer.writerow(columns)
    writer.writerows(rows)
    file.seek(0)
    
    _upload_to_s3(file, f'weekly/raw/marshavesent_raw_{week_start}_to_{week_end}.csv')

def export_aggregated_to_csv(week_start=None, week_end=None, **context):
    """Экспортирует агрегированные недельные данные в CSV."""
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import codecs
    
    if week_start is None or week_end is None:
        ti = context.get('task_instance')
        if ti:
            week_start = ti.xcom_pull(task_ids='get_week_boundaries', key='week_start')
            week_end = ti.xcom_pull(task_ids='get_week_boundaries', key='week_end')
    
    with _get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT lti_user_id, attempt_type, lesson_id, lesson_name,
                      attempt_count, correct_count, incorrect_count,
                      avg_score, max_score, min_score, stddev_score,
                      first_attempt, last_attempt, week_start, week_end
               FROM marshavesent_agg_data
               WHERE week_start = %s::timestamp
               ORDER BY lti_user_id, attempt_type""",
            (week_start,)
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"Выгружено {len(rows)} строк для CSV")
    
    file = BytesIO()
    writer = csv.writer(codecs.getwriter('utf-8')(file))
    writer.writerow(columns)
    writer.writerows(rows)
    file.seek(0)
    
    _upload_to_s3(file, f'weekly/agg/marshavesent_agg_{week_start}_to_{week_end}.csv')

# ============================================
# ФУНКЦИИ ДЛЯ МЕСЯЧНОЙ ЗАГРУЗКИ
# ============================================

def load_raw_data_monthly(month_start=None, month_end=None, **context):
    """
    Загружает сырые данные за месяц из API.
    Поддерживает Jinja через op_kwargs И params (обратная совместимость).
    """
    import requests
    import psycopg2 as pg
    import ast
    
    # Приоритет: op_kwargs > params > XCom
    if month_start is None or month_end is None:
        # Пробуем params (для обратной совместимости)
        params = context.get('params', {})
        if params:
            month_start = params.get('month_start')
            month_end = params.get('month_end')
    
    if not month_start or not month_end:
        raise ValueError(f"Не указаны month_start={month_start} или month_end={month_end}")
    
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': month_start,
        'end': month_end
    }
    
    print(f"Запрос месячных данных из API: {month_start} до {month_end}")
    response = requests.get(API_URL, params=payload, timeout=60)
    response.raise_for_status()
    data = response.json()
    print(f"Получено {len(data)} записей за месяц")
    
    if not data:
        print("ПРЕДУПРЕЖДЕНИЕ: API вернул пустой список данных!")
        return
    
    with _get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Создаем таблицу
        cursor.execute(CREATE_MONTHLY_TABLE_SQL)
        
        # Идемпотентность
        cursor.execute(
            """DELETE FROM marshavesent_raw_data_monthly 
               WHERE created_at >= %s::timestamp 
               AND created_at < %s::timestamp + interval '1 day'""",
            (month_start, month_end)
        )
        print(f"Очищены данные за месяц {month_start} - {month_end}")
        
        # Вставка
        insert_sql = """
            INSERT INTO marshavesent_raw_data_monthly 
            (lti_user_id, is_correct, attempt_type, created_at, lesson_id, lesson_name, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        inserted = 0
        for el in data:
            pp = _parse_passback_params(el.get('passback_params'))
            cursor.execute(insert_sql, (
                el.get('lti_user_id'),
                True if el.get('is_correct') == 1 else False,
                el.get('attempt_type'),
                el.get('created_at'),
                pp.get('lesson_id'),
                pp.get('lesson_name'),
                pp.get('score') or el.get('score')
            ))
            inserted += 1
        
        conn.commit()
        print(f"Вставлено {inserted} записей в месячную таблицу")

def export_raw_monthly_to_csv(month_start=None, month_end=None, **context):
    """Экспортирует сырые месячные данные в CSV."""
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import codecs
    
    # Приоритет: op_kwargs > params
    if month_start is None or month_end is None:
        params = context.get('params', {})
        if params:
            month_start = params.get('month_start')
            month_end = params.get('month_end')
    
    if not month_start or not month_end:
        raise ValueError(f"Не указаны month_start={month_start} или month_end={month_end}")
    
    with _get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT lti_user_id, attempt_type, is_correct, created_at, lesson_id, lesson_name, score
               FROM marshavesent_raw_data_monthly
               WHERE created_at >= %s::timestamp AND created_at < %s::timestamp + interval '1 day'
               ORDER BY created_at""",
            (month_start, month_end)
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"Выгружено {len(rows)} строк для месячного CSV")
    
    file = BytesIO()
    writer = csv.writer(codecs.getwriter('utf-8')(file))
    writer.writerow(columns)
    writer.writerows(rows)
    file.seek(0)
    
    _upload_to_s3(file, f'monthly/raw/marshavesent_monthly_raw_{month_start}_to_{month_end}.csv')