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

        start_drwNo = 1082
        while True:
            self.log.info(f'시작:{start_drwNo}')
            row_df = self._call_api(self.base_url, start_drwNo)
            
            if self.time == row_df.loc[0,'drwNoDate'].replace("-",""):
                
                # data.append({'toSellamnt':row_df.loc[0][0],'returnvalue':row_df.loc[0][1],'drwNoDate':row_df.loc[0][2],'firstWinamnt':row_df.loc[0][3],
                #              'drwNo6':row_df.loc[0][4],'drwtNo4':row_df.loc[0][5],'firstPrzwnerCo':row_df.loc[0][6],'drwNo5':row_df.loc[0][7],
                #              'bnsNo':row_df.loc[0][8],'firstAccumant':row_df.loc[0][9],'drwNo':row_df.loc[0][10],'drwNo2':row_df.loc[0][11],
                #              'drwtNo3':row_df.loc[0][12],'drwtNo1':row_df.loc[0][13]}, ignore_index=True)

                new_df = pd.concat([data,row_df], ignore_index= True)
                break
            else: 
                 start_drwNo+=1

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        new_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)
        
    def _call_api(self, base_url, drwNo):
            import requests
            import json

            headers={'Content-Type': 'application/json',
                    'charset': 'utf-8',
                    'Accept': '*/*'}
            request_url = f'{base_url}={drwNo}'
            
            response = requests.get(request_url, headers)
            
            contents = json.loads(response.text)

            row_df = pd.DataFrame([contents.values()], columns = ['toSellamnt','returnvalue','drwNoDate','firstWinamnt','drwNo6','drwtNo4','firstPrzwnerCo','drwNo5',
                                                                'bnsNo','firstAccumant','drwNo','drwNo2','drwtNo3','drwtNo1'])
            print(row_df)
            return row_df
