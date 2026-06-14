import requests
import psycopg2
from typing import Any

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context


class ExtractDataOperator(BaseOperator):

    # без этого {{ ds }} не отрендерится в start_date и end_date
    template_fields = ('start_date', 'end_date')

    API_URL = "https://b2b.itresume.ru/api/statistics"

    def __init__(
        self,
        start_date: str,
        end_date: str,
        pg_conn_id: str = "conn_pg",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date
        self.pg_conn_id = pg_conn_id

    def execute(self, context: Context) -> Any:
        self.log.info(f"забираем данные за период: {self.start_date} — {self.end_date}")

        response = requests.get(
            self.API_URL,
            params={
                "client": "Skillfactory",
                "client_key": "M2MGWS",
                "start": self.start_date,
                "end": self.end_date
            }
        )
        response.raise_for_status()
        data = response.json()
        self.log.info(f"апи вернул записей: {len(data)}")

        conn = BaseHook.get_connection(self.pg_conn_id)
        db = psycopg2.connect(
            host=conn.host,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            port=conn.port
        )
        cur = db.cursor()

        inserted = 0
        for row in data:
            cur.execute("""
                INSERT INTO reylife_raw (
                    lti_user_id,
                    passback_params,
                    is_correct,
                    attempt_type,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                row.get('lti_user_id'),
                row.get('passback_params'),
                row.get('is_correct'),
                row.get('attempt_type'),
                row.get('created_at')
            ))

            if cur.rowcount:
                inserted += 1

        db.commit()
        db.close()

        self.log.info(f"вставили новых строк: {inserted}")
        return inserted