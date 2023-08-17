from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id = 'dags_simple_http_operator',
    start_date = pendulum.datetime(2023,8,1,tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:
    '''서울시 도서관 강좌 정보'''
    task_library_lec_info = SimpleHttpOperator(
        task_id = 'task_library_lec_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/TbCorona19CountStatus/1/5/',
        method='GET',
        headers={'Content-Type': 'application/json',
                'charset': 'utf-8',
                'Accept': '*/*'}

    )

    @task(task_id = 'python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        res = ti.xcom_pull(task_ids='task_library_lec_info')
        import json
        from pprint import pprint
        
        pprint(json.loads(res))

    task_library_lec_info >> python_2()