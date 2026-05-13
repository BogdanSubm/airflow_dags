"""
Конфигурация для динамической генерации DAG.
Чтобы добавить новую таблицу - добавьте словарь в TABLE_CONFIGS.
"""

from datetime import datetime

DAG_CONFIG = {
    'schedule_interval': '@daily',
    'start_date': datetime(2026, 1, 1),
    'catchup': True,
    'max_active_runs': 1,
    'max_active_tasks': 3,  # Параллелим выполнение разных таблиц
    'tags': ['marshavesent', 'dynamic'],
    'description': 'Динамическая генерация таблиц на основе конфигурации',
}

CONNECTION_CONFIG = {
    'pg_conn_id': 'conn_pg',
    'pg_db': 'etl',
    's3_conn_id': 'conn_s3',
    's3_bucket': 'marshavesent-bucket',
}

TABLE_CONFIGS = [
    {
        'table_name': 'marshavesent_user_attempts_summary',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS marshavesent_user_attempts_summary (
                id SERIAL PRIMARY KEY,
                lti_user_id VARCHAR(255),
                attempt_type VARCHAR(50),
                total_attempts INTEGER,
                correct_attempts INTEGER,
                incorrect_attempts INTEGER,
                avg_score DECIMAL(10,2),
                last_attempt_date TIMESTAMP,
                load_date DATE DEFAULT CURRENT_DATE,
                UNIQUE(lti_user_id, attempt_type, load_date)
            )
        ''',
        'table_dml': '''
            INSERT INTO marshavesent_user_attempts_summary 
            (lti_user_id, attempt_type, total_attempts, correct_attempts, 
             incorrect_attempts, avg_score, last_attempt_date, load_date)
            SELECT 
                lti_user_id,
                attempt_type,
                COUNT(*) as total_attempts,
                COUNT(CASE WHEN is_correct THEN 1 END) as correct_attempts,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) as incorrect_attempts,
                ROUND(AVG(score)::numeric, 2) as avg_score,
                MAX(created_at) as last_attempt_date,
                '{{ ds }}'::date as load_date
            FROM marshavesent_raw_data
            WHERE created_at::date = '{{ ds }}'::date
            GROUP BY lti_user_id, attempt_type
            ON CONFLICT (lti_user_id, attempt_type, load_date) 
            DO UPDATE SET
                total_attempts = EXCLUDED.total_attempts,
                correct_attempts = EXCLUDED.correct_attempts,
                incorrect_attempts = EXCLUDED.incorrect_attempts,
                avg_score = EXCLUDED.avg_score,
                last_attempt_date = EXCLUDED.last_attempt_date
        ''',
        'need_to_export': True,
        'export_prefix': 'user_attempts'
    },
    {
        'table_name': 'marshavesent_lesson_stats',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS marshavesent_lesson_stats (
                id SERIAL PRIMARY KEY,
                lesson_id VARCHAR(255),
                lesson_name VARCHAR(500),
                total_attempts INTEGER,
                unique_users INTEGER,
                success_rate DECIMAL(5,2),
                avg_score DECIMAL(10,2),
                load_date DATE DEFAULT CURRENT_DATE,
                UNIQUE(lesson_id, load_date)
            )
        ''',
        'table_dml': '''
            INSERT INTO marshavesent_lesson_stats 
            (lesson_id, lesson_name, total_attempts, unique_users, 
             success_rate, avg_score, load_date)
            SELECT 
                lesson_id,
                MAX(lesson_name) as lesson_name,
                COUNT(*) as total_attempts,
                COUNT(DISTINCT lti_user_id) as unique_users,
                ROUND(
                    COUNT(CASE WHEN is_correct THEN 1 END)::decimal / 
                    NULLIF(COUNT(*), 0) * 100, 2
                ) as success_rate,
                ROUND(AVG(score)::numeric, 2) as avg_score,
                '{{ ds }}'::date as load_date
            FROM marshavesent_raw_data
            WHERE created_at::date = '{{ ds }}'::date
                AND lesson_id IS NOT NULL
            GROUP BY lesson_id
            ON CONFLICT (lesson_id, load_date) 
            DO UPDATE SET
                lesson_name = EXCLUDED.lesson_name,
                total_attempts = EXCLUDED.total_attempts,
                unique_users = EXCLUDED.unique_users,
                success_rate = EXCLUDED.success_rate,
                avg_score = EXCLUDED.avg_score
        ''',
        'need_to_export': True,
        'export_prefix': 'lesson_stats'
    },
    {
        'table_name': 'marshavesent_hourly_activity',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS marshavesent_hourly_activity (
                id SERIAL PRIMARY KEY,
                activity_hour INTEGER,
                attempt_type VARCHAR(50),
                attempt_count INTEGER,
                unique_users INTEGER,
                load_date DATE DEFAULT CURRENT_DATE,
                UNIQUE(activity_hour, attempt_type, load_date)
            )
        ''',
        'table_dml': '''
            INSERT INTO marshavesent_hourly_activity 
            (activity_hour, attempt_type, attempt_count, unique_users, load_date)
            SELECT 
                EXTRACT(HOUR FROM created_at)::integer as activity_hour,
                attempt_type,
                COUNT(*) as attempt_count,
                COUNT(DISTINCT lti_user_id) as unique_users,
                '{{ ds }}'::date as load_date
            FROM marshavesent_raw_data
            WHERE created_at::date = '{{ ds }}'::date
            GROUP BY EXTRACT(HOUR FROM created_at), attempt_type
            ON CONFLICT (activity_hour, attempt_type, load_date) 
            DO UPDATE SET
                attempt_count = EXCLUDED.attempt_count,
                unique_users = EXCLUDED.unique_users
        ''',
        'need_to_export': False
    },
]