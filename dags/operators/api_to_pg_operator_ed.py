import requests
import psycopg2 as pg
import ast
import pendulum
from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

class APIToPgOperator(BaseOperator):
    URL = 'https://b2b.itresume.ru/api/statistics'
    
    template_fields = ('date_from', 'date_to')

    def __init__(self, date_from: str, date_to: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to

    def execute(self, context):
        dt_from = pendulum.parse(self.date_from)
        dt_from = dt_from.start_of('month')
        dt_to = pendulum.parse(self.date_to)
        dt_to = dt_to.end_of('month')

        self.date_from = dt_from.to_date_string()
        self.date_to = dt_to.to_date_string()

        payload = {
            'client': 'Skillfactory',
            'client_key': 'M2MGWS',
            'start': self.date_from,
            'end': self.date_to
        }
        response = requests.get(self.URL, params=payload)
        data = response.json()

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

            rows_to_insert = []
            for el in data:
                dt = datetime.strptime(el.get('created_at'), "%Y-%m-%d %H:%M:%S.%f")
                user = el.get('lti_user_id')
                cursor.execute("""SELECT 1 FROM raw_data_ed WHERE lti_user_id = %s AND created_at = %s""", (user, dt))
                if not cursor.fetchone():
                    row = []
                    passback_params = ast.literal_eval(el.get('passback_params') if el.get('passback_params') else '{}')
                    row.append(el.get('lti_user_id'))
                    row.append(True if el.get('is_correct') == 1 else False)
                    row.append(el.get('attempt_type'))
                    row.append(el.get('created_at'))
                    row.append(passback_params.get('oauth_consumer_key'))
                    row.append(passback_params.get('lis_result_sourcedid'))
                    row.append(passback_params.get('lis_outcome_service_url'))
                    rows_to_insert.append(row)
            
            if rows_to_insert:
                cursor.executemany("""INSERT INTO raw_data_ed VALUES (%s, %s, %s, %s, %s, %s, %s)""", rows_to_insert)
            conn.commit()