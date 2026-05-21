from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


def get_conn():
    import psycopg2
    creds = BaseHook.get_connection('postgres_bubble')
    return psycopg2.connect(
        dbname='etl',
        sslmode='disable',
        user=creds.login,
        password=creds.password,
        host=creds.host,
        port=creds.port,
        connect_timeout=600,
        tcp_user_timeout=600,
    )


def load_from_api(**context):
    import requests
    import ast

    ds = context['ds']
    
    end_date = (datetime.strptime(ds, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': ds,
        'end': end_date,
    }

    response = requests.get(url='https://b2b.itresume.ru/api/statistics', params=payload)
    data = response.json()

    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM soldatowiw_raw_table WHERE ds = %s', (ds,))

        for el in data:
            passback_params = ast.literal_eval(el.get('passback_params') or '{}')
            row = [
                el.get('lti_user_id'),
                True if el.get('is_correct') == 1 else False,
                el.get('attempt_type'),
                el.get('created_at'),
                passback_params.get('oauth_consumer_key'),
                passback_params.get('lis_result_sourcedid'),
                passback_params.get('lis_outcome_service_url'),
                ds,
            ]
            cursor.execute(
                'INSERT INTO soldatowiw_raw_table '
                '(lti_user_id, is_correct, attempt_type, created_at, '
                'oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, ds) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                row,
            )

        conn.commit()


def aggregate(**context):
    ds = context['ds']
    period_end = (datetime.strptime(ds, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                COUNT(*)                                                    AS total_attempts,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END)                AS correct_attempts,
                SUM(CASE WHEN NOT is_correct THEN 1 ELSE 0 END)            AS incorrect_attempts,
                COUNT(DISTINCT lti_user_id)                                 AS unique_users,
                ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END), 4)  AS correct_rate
            FROM soldatowiw_raw_table
            WHERE ds = %s
            ''',
            (ds,),
        )
        row = cursor.fetchone()

        cursor.execute(
            '''
            INSERT INTO soldatowiw_agg_table
                (period_start, period_end, total_attempts, correct_attempts,
                 incorrect_attempts, unique_users, correct_rate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (period_start, period_end) DO UPDATE SET
                total_attempts = EXCLUDED.total_attempts,
                correct_attempts = EXCLUDED.correct_attempts,
                incorrect_attempts = EXCLUDED.incorrect_attempts,
                unique_users = EXCLUDED.unique_users,
                correct_rate = EXCLUDED.correct_rate
            ''',
            (ds, period_end) + row,
        )

        conn.commit()
