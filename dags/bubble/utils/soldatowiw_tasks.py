from airflow.hooks.base import BaseHook


def get_conn():
    import psycopg2
    creds = BaseHook.get_connection('conn_pg')
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


def load_from_api(ds, end_date):
    import requests
    import ast

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


def export_to_s3(table_name, ds):
    # Выгрузка содержимого таблицы за период ds в объектное хранилище в CSV.
    # Тяжёлые импорты внутри функции (см. CLAUDE.md).
    import csv
    import io

    from bubble.utils.soldatowiw_s3 import get_s3_client

    with get_conn() as conn:
        cursor = conn.cursor()
        # table_name из доверенного config (не пользовательский ввод) → конкатенация.
        cursor.execute('SELECT * FROM ' + table_name + ' WHERE ds = %s', (ds,))
        rows = cursor.fetchall()
        header = [col.name for col in cursor.description]

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)

    s3_client = get_s3_client()
    # Один и тот же ключ на каждый запуск за ds → перезапись, экспорт идемпотентен.
    s3_client.put_object(
        Body=buffer.getvalue().encode('utf-8'),
        Bucket='default-storage',
        Key='soldatowiw/' + table_name + '/' + ds + '.csv',
    )


def aggregate(ds, period_end):

    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                COUNT(*) AS total_attempts,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS correct_attempts,
                SUM(CASE WHEN NOT is_correct THEN 1 ELSE 0 END) AS incorrect_attempts,
                COUNT(DISTINCT lti_user_id) AS unique_users,
                ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END), 4) AS correct_rate
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
