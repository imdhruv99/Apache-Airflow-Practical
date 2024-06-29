import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class HttpGetOperator(BaseOperator):
    @apply_defaults
    def __init__(self, endpoint, *args, **kwargs):
        super(HttpGetOperator, self).__init__(*args, **kwargs)
        self.endpoint = endpoint

    def execute(self, context):
        response = requests.get(self.endpoint)
        if response.status_code == 200:
            self.log.info(f"Success: {response.status_code}")
            return response.json()
        else:
            self.log.error(f"Failed: {response.status_code}")
            raise ValueError(F"Request to {self.endpoint} failed with status {response.status_code}")