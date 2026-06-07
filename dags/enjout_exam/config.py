"""
Конфигурация динамических агрегатов.

Чтобы добавить новую таблицу — достаточно дописать словарь в TABLE_CONFIGS.
DAG автоматически создаст TaskGroup: create → fill → quality → export.
"""

from datetime import datetime

RAW_TABLE = 'enjout_raw_data'

DAG_CONFIG = {
    'dag_id': 'enjout_exam_dynamic_dag',
    'schedule': '0 7 * * 1',
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
    'max_active_runs': 1,
    'max_active_tasks': 3,
    'tags': ['enjout', 'enjout_exam', 'dynamic', 'final'],
    'description': 'Динамическая генерация агрегатов на основе Python-конфига',
}

S3_BUCKET = 'default-storage'
S3_PREFIX = 'enjout_exam'

TABLE_CONFIGS = [
    {
        'table_name': 'enjout_exam_by_attempt_type',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS enjout_exam_by_attempt_type (
                attempt_type VARCHAR(50),
                total_attempts INTEGER,
                correct_attempts INTEGER,
                incorrect_attempts INTEGER,
                unique_users INTEGER,
                avg_correct_rate DECIMAL(10, 4),
                min_created_at TIMESTAMP,
                max_created_at TIMESTAMP,
                period_start DATE,
                period_end DATE,
                UNIQUE(attempt_type, period_start, period_end)
            )
        ''',
        'table_dml': '''
            SELECT
                attempt_type,
                COUNT(*) AS total_attempts,
                COUNT(CASE WHEN is_correct THEN 1 END) AS correct_attempts,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) AS incorrect_attempts,
                COUNT(DISTINCT lti_user_id) AS unique_users,
                ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END), 4) AS avg_correct_rate,
                MIN(created_at) AS min_created_at,
                MAX(created_at) AS max_created_at,
                '{{ previous_week_start(ds) }}'::date AS period_start,
                '{{ previous_week_end(ds) }}'::date AS period_end
            FROM enjout_raw_data
            WHERE period_start = '{{ previous_week_start(ds) }}'::date
              AND period_end = '{{ previous_week_end(ds) }}'::date
            GROUP BY attempt_type
        ''',
        'need_to_export': True,
        'min_rows': 0,
    },
    {
        'table_name': 'enjout_exam_by_user',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS enjout_exam_by_user (
                lti_user_id VARCHAR(255),
                attempt_type VARCHAR(50),
                total_attempts INTEGER,
                correct_attempts INTEGER,
                incorrect_attempts INTEGER,
                avg_correct_rate DECIMAL(10, 4),
                period_start DATE,
                period_end DATE,
                UNIQUE(lti_user_id, attempt_type, period_start, period_end)
            )
        ''',
        'table_dml': '''
            SELECT
                lti_user_id,
                attempt_type,
                COUNT(*) AS total_attempts,
                COUNT(CASE WHEN is_correct THEN 1 END) AS correct_attempts,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) AS incorrect_attempts,
                ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END), 4) AS avg_correct_rate,
                '{{ previous_week_start(ds) }}'::date AS period_start,
                '{{ previous_week_end(ds) }}'::date AS period_end
            FROM enjout_raw_data
            WHERE period_start = '{{ previous_week_start(ds) }}'::date
              AND period_end = '{{ previous_week_end(ds) }}'::date
            GROUP BY lti_user_id, attempt_type
        ''',
        'need_to_export': False,
        'min_rows': 0,
    },
    {
        'table_name': 'enjout_exam_weekly_summary',
        'table_ddl': '''
            CREATE TABLE IF NOT EXISTS enjout_exam_weekly_summary (
                total_attempts INTEGER,
                correct_attempts INTEGER,
                incorrect_attempts INTEGER,
                unique_users INTEGER,
                unique_attempt_types INTEGER,
                avg_correct_rate DECIMAL(10, 4),
                min_created_at TIMESTAMP,
                max_created_at TIMESTAMP,
                period_start DATE,
                period_end DATE,
                UNIQUE(period_start, period_end)
            )
        ''',
        'table_dml': '''
            SELECT
                COUNT(*) AS total_attempts,
                COUNT(CASE WHEN is_correct THEN 1 END) AS correct_attempts,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) AS incorrect_attempts,
                COUNT(DISTINCT lti_user_id) AS unique_users,
                COUNT(DISTINCT attempt_type) AS unique_attempt_types,
                ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END), 4) AS avg_correct_rate,
                MIN(created_at) AS min_created_at,
                MAX(created_at) AS max_created_at,
                '{{ previous_week_start(ds) }}'::date AS period_start,
                '{{ previous_week_end(ds) }}'::date AS period_end
            FROM enjout_raw_data
            WHERE period_start = '{{ previous_week_start(ds) }}'::date
              AND period_end = '{{ previous_week_end(ds) }}'::date
        ''',
        'need_to_export': True,
        'min_rows': 1,
    },
]
