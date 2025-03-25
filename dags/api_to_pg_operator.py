import ast

import psycopg2 as pg
import requests
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator


class APIToPgOperator(BaseOperator):
    API_URL = "https://b2b.itresume.ru/api/statistics"

    template_fields = ('date_from', 'date_to')

    def __init__(self, date_from: str, date_to: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to

    def execute(self, context):
        payload = {
            'client': 'Skillfactory',
            'client_key': 'M2MGWS',
            'start': self.date_from,
            'end': self.date_to,
        }
        response = requests.get(self.API_URL, params=payload)
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

            for el in data:
                row = []
                passback_params = ast.literal_eval(el.get('passback_params') if el.get('passback_params') else '{}')
                row.append(el.get('lti_user_id'))
                row.append(True if el.get('is_correct') == 1 else False)
                row.append(el.get('attempt_type'))
                row.append(el.get('created_at'))
                row.append(passback_params.get('oauth_consumer_key'))
                row.append(passback_params.get('lis_result_sourcedid'))
                row.append(passback_params.get('lis_outcome_service_url'))

                cursor.execute("INSERT INTO admin_table VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

            conn.commit()