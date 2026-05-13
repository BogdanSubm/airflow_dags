"""
Конфигурация для динамической генерации DAG с помесячной агрегацией.
Чтобы добавить новую таблицу - добавьте словарь в TABLE_CONFIGS.

"""

from datetime import datetime

DAG_CONFIG = {
    'schedule_interval': '@daily',  
    'start_date': datetime(2026, 5, 1),  
    'catchup': False,  
    'max_active_runs': 1,
    'max_active_tasks': 3,  
    'tags': ['marshavesent', 'dynamic', 'monthly'],
    'description': 'Динамическая генерация таблиц с помесячной агрегацией',
}

CONNECTION_CONFIG = {
    'pg_conn_id': 'conn_pg',
    'pg_db': 'etl',
    's3_conn_id': 'conn_s3',
    's3_bucket': 'marshavesent-bucket',
}

TABLE_CONFIGS = [
    {
        'table_name': 'marshavesent_user_attempts_monthly',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS marshavesent_user_attempts_monthly (
                id SERIAL PRIMARY KEY,
                lti_user_id VARCHAR(255),
                attempt_type VARCHAR(50),
                total_attempts INTEGER,
                correct_attempts INTEGER,
                incorrect_attempts INTEGER,
                avg_score DECIMAL(10,2),
                max_score DECIMAL(10,2),
                min_score DECIMAL(10,2),
                first_attempt_date TIMESTAMP,
                last_attempt_date TIMESTAMP,
                year_month VARCHAR(7),
                load_date DATE DEFAULT CURRENT_DATE,
                UNIQUE(lti_user_id, attempt_type, year_month)
            )
        ''',
        'table_dml': '''
            INSERT INTO marshavesent_user_attempts_monthly 
            (lti_user_id, attempt_type, total_attempts, correct_attempts, 
             incorrect_attempts, avg_score, max_score, min_score,
             first_attempt_date, last_attempt_date, year_month, load_date)
            SELECT 
                lti_user_id,
                attempt_type,
                COUNT(*) as total_attempts,
                COUNT(CASE WHEN is_correct THEN 1 END) as correct_attempts,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) as incorrect_attempts,
                ROUND(AVG(score)::numeric, 2) as avg_score,
                MAX(score) as max_score,
                MIN(score) as min_score,
                MIN(created_at) as first_attempt_date,
                MAX(created_at) as last_attempt_date,
                TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') as year_month,
                '{{ ds }}'::date as load_date
            FROM marshavesent_raw_data
            WHERE created_at >= DATE_TRUNC('month', '{{ ds }}'::date)
                AND created_at < DATE_TRUNC('month', '{{ ds }}'::date) + INTERVAL '1 month'
            GROUP BY lti_user_id, attempt_type, TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM')
            ON CONFLICT (lti_user_id, attempt_type, year_month) 
            DO UPDATE SET
                total_attempts = EXCLUDED.total_attempts,
                correct_attempts = EXCLUDED.correct_attempts,
                incorrect_attempts = EXCLUDED.incorrect_attempts,
                avg_score = EXCLUDED.avg_score,
                max_score = EXCLUDED.max_score,
                min_score = EXCLUDED.min_score,
                first_attempt_date = EXCLUDED.first_attempt_date,
                last_attempt_date = EXCLUDED.last_attempt_date,
                load_date = EXCLUDED.load_date
        ''',
        'need_to_export': True,
        'export_prefix': 'user_attempts_monthly'
    },
    {
        'table_name': 'marshavesent_lesson_stats_monthly',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS marshavesent_lesson_stats_monthly (
                id SERIAL PRIMARY KEY,
                lesson_id VARCHAR(255),
                lesson_name VARCHAR(500),
                total_attempts INTEGER,
                unique_users INTEGER,
                success_rate DECIMAL(5,2),
                avg_score DECIMAL(10,2),
                max_score DECIMAL(10,2),
                min_score DECIMAL(10,2),
                year_month VARCHAR(7),
                load_date DATE DEFAULT CURRENT_DATE,
                UNIQUE(lesson_id, year_month)
            )
        ''',
        'table_dml': '''
            INSERT INTO marshavesent_lesson_stats_monthly 
            (lesson_id, lesson_name, total_attempts, unique_users, 
             success_rate, avg_score, max_score, min_score, year_month, load_date)
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
                MAX(score) as max_score,
                MIN(score) as min_score,
                TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') as year_month,
                '{{ ds }}'::date as load_date
            FROM marshavesent_raw_data
            WHERE created_at >= DATE_TRUNC('month', '{{ ds }}'::date)
                AND created_at < DATE_TRUNC('month', '{{ ds }}'::date) + INTERVAL '1 month'
                AND lesson_id IS NOT NULL
            GROUP BY lesson_id, TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM')
            ON CONFLICT (lesson_id, year_month) 
            DO UPDATE SET
                lesson_name = EXCLUDED.lesson_name,
                total_attempts = EXCLUDED.total_attempts,
                unique_users = EXCLUDED.unique_users,
                success_rate = EXCLUDED.success_rate,
                avg_score = EXCLUDED.avg_score,
                max_score = EXCLUDED.max_score,
                min_score = EXCLUDED.min_score,
                load_date = EXCLUDED.load_date
        ''',
        'need_to_export': True,
        'export_prefix': 'lesson_stats_monthly'
    },
    {
        'table_name': 'marshavesent_daily_activity_monthly',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS marshavesent_daily_activity_monthly (
                id SERIAL PRIMARY KEY,
                activity_date DATE,
                attempt_type VARCHAR(50),
                attempt_count INTEGER,
                unique_users INTEGER,
                correct_count INTEGER,
                incorrect_count INTEGER,
                year_month VARCHAR(7),
                load_date DATE DEFAULT CURRENT_DATE,
                UNIQUE(activity_date, attempt_type, year_month)
            )
        ''',
        'table_dml': '''
            INSERT INTO marshavesent_daily_activity_monthly 
            (activity_date, attempt_type, attempt_count, unique_users,
             correct_count, incorrect_count, year_month, load_date)
            SELECT 
                created_at::date as activity_date,
                attempt_type,
                COUNT(*) as attempt_count,
                COUNT(DISTINCT lti_user_id) as unique_users,
                COUNT(CASE WHEN is_correct THEN 1 END) as correct_count,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) as incorrect_count,
                TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') as year_month,
                '{{ ds }}'::date as load_date
            FROM marshavesent_raw_data
            WHERE created_at >= DATE_TRUNC('month', '{{ ds }}'::date)
                AND created_at < DATE_TRUNC('month', '{{ ds }}'::date) + INTERVAL '1 month'
            GROUP BY created_at::date, attempt_type, 
                     TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM')
            ON CONFLICT (activity_date, attempt_type, year_month) 
            DO UPDATE SET
                attempt_count = EXCLUDED.attempt_count,
                unique_users = EXCLUDED.unique_users,
                correct_count = EXCLUDED.correct_count,
                incorrect_count = EXCLUDED.incorrect_count,
                load_date = EXCLUDED.load_date
        ''',
        'need_to_export': True,
        'export_prefix': 'daily_activity_monthly'
    },
    {
        'table_name': 'marshavesent_monthly_summary',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS marshavesent_monthly_summary (
                id SERIAL PRIMARY KEY,
                year_month VARCHAR(7),
                total_users INTEGER,
                total_attempts INTEGER,
                total_correct INTEGER,
                total_incorrect INTEGER,
                overall_success_rate DECIMAL(5,2),
                avg_score DECIMAL(10,2),
                active_days INTEGER,
                first_date TIMESTAMP,
                last_date TIMESTAMP,
                load_date DATE DEFAULT CURRENT_DATE,
                UNIQUE(year_month)
            )
        ''',
        'table_dml': '''
            INSERT INTO marshavesent_monthly_summary 
            (year_month, total_users, total_attempts, total_correct, 
             total_incorrect, overall_success_rate, avg_score,
             active_days, first_date, last_date, load_date)
            SELECT 
                TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') as year_month,
                COUNT(DISTINCT lti_user_id) as total_users,
                COUNT(*) as total_attempts,
                COUNT(CASE WHEN is_correct THEN 1 END) as total_correct,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) as total_incorrect,
                ROUND(
                    COUNT(CASE WHEN is_correct THEN 1 END)::decimal / 
                    NULLIF(COUNT(*), 0) * 100, 2
                ) as overall_success_rate,
                ROUND(AVG(score)::numeric, 2) as avg_score,
                COUNT(DISTINCT created_at::date) as active_days,
                MIN(created_at) as first_date,
                MAX(created_at) as last_date,
                '{{ ds }}'::date as load_date
            FROM marshavesent_raw_data
            WHERE created_at >= DATE_TRUNC('month', '{{ ds }}'::date)
                AND created_at < DATE_TRUNC('month', '{{ ds }}'::date) + INTERVAL '1 month'
            GROUP BY TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM')
            ON CONFLICT (year_month) 
            DO UPDATE SET
                total_users = EXCLUDED.total_users,
                total_attempts = EXCLUDED.total_attempts,
                total_correct = EXCLUDED.total_correct,
                total_incorrect = EXCLUDED.total_incorrect,
                overall_success_rate = EXCLUDED.overall_success_rate,
                avg_score = EXCLUDED.avg_score,
                active_days = EXCLUDED.active_days,
                first_date = EXCLUDED.first_date,
                last_date = EXCLUDED.last_date,
                load_date = EXCLUDED.load_date
        ''',
        'need_to_export': True,
        'export_prefix': 'monthly_summary'
    },
]