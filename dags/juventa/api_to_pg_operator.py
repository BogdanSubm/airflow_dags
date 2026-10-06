import ast

import psycopg2 as pg
import requests
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

class JApiToPgOperator(BaseOperator):
    API_URL = "https://b2b.itresume.ru/api/statistics"

    # Для jinja объявляем template_fields
    template_fields = ('date_from', 'date_to')

    #Инициализация
    def __init__(self, date_from:str, date_to:str,  **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from  #Объявляю переменные
        self.date_to = date_to      #Объявляю переменные

    # То что будет выполняться при вызове оператора
    def execute(self, context):
        payload = {
            'client': 'Skillfactory',
            'client_key': 'M2MGWS',
            'start': self.date_from,
            'end': self.date_to
        }
        response = requests.get(self.API_URL, params=payload, timeout=60)
        response.raise_for_status()  # если API вернет ошибку (404, 500), код упадет здесь с понятной причиной, а не при парсинге JSON
        data = response.json()

        # данные которые ранее получили из API
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
            delete_query = '''
                        DELETE FROM juventa_raw_month_table
                        where created_at >= %s::timestamp
                          and created_at < %s::timestamp + interval '1 day'        
                        '''
            cursor.execute(delete_query, (self.date_from, self.date_to))
            print(f"Удалены данные в БД PostgreSQL за период с {self.date_from} по {self.date_to}")

            for el in data:
                row = []
                passback_params = ast.literal_eval(el.get('passback_params') or '{}')
                row.append(el.get('lti_user_id'))
                row.append(True if el.get('is_correct') == 1 else False)
                row.append(el.get('attempt_type'))
                row.append(el.get('created_at'))
                row.append(passback_params.get('oauth_consumer_key'))
                row.append(passback_params.get('lis_result_sourcedid'))
                row.append(passback_params.get('lis_outcome_service_url'))

                cursor.execute("INSERT INTO juventa_raw_month_table VALUES(%s, %s, %s, %s, %s, %s, %s)", row)
            conn.commit()
            print('Сырые данные сохранены в БД PostgreSQL')





