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
        print(self.time)
        
        #'파일경로/파일이름.csv'
        data = pd.read_csv(self.file)
        print(data)

        start_drwNo = 1080
        while True:
            self.log.info(f'시작:{start_drwNo}')
            row_df = self._call_api(self.base_url, start_drwNo)
            print(row_df)
            print(row_df.loc[0][2].replace("-",""))
            if self.time == row_df.loc[0][2].replace("-",""):
                print("확인")
                data = pd.concat([data, row_df])
                break
            else: start_drwNo+=1

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        data.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)
        
    def _call_api(self, base_url, drwNo):
            import requests
            import json

            headers={'Content-Type': 'application/json',
                    'charset': 'utf-8',
                    'Accept': '*/*'}
            request_url = f'{base_url}={drwNo}'
            
            response = requests.get(request_url, headers)
            
            contents = json.loads(response.text)

            row_df = pd.DataFrame([contents.values()], index = [0])

            return row_df
