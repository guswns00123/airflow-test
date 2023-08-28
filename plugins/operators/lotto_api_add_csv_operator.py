from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 

class LottoApiAddCsvOperator(BaseOperator):
    template_fields = template_fields = ('endpoint', 'file', 'path', 'file_name', 'time')

    def __init__(self, path, file_name, time, file, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'www.dhlottery.co.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = 'common.do?method=getLottoNumber&drwNo'
        self.file = file
        self.time = time

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}/{self.endpoint}'
        total_row_df = pd.DataFrame()
        print(self.time)
        #'파일경로/파일이름.csv'
        data = pd.read_csv(self.file)


