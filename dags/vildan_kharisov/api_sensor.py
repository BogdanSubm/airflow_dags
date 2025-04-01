from airflow.sensors.base import BaseSensorOperator
import requests


class APISensor(BaseSensorOperator):
    API_URL = "https://b2b.itresume.ru/api/statistics"

    template_fields = ('date_from', 'date_to')

    def __init__(self, date_from: str, date_to: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to

    def poke(self,  context) -> bool:
        payload = {
            'client': 'Skillfactory',
            'client_key': 'M2MGWS',
            'start': self.date_from,
            'end': self.date_to,
        }
        response = requests.get(self.API_URL, params=payload)
        data = response.json()

        if data:
            return True
        else:
            return False