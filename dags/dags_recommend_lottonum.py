from airflow import DAG
from airflow.operators.python import PythonOperator
from common.predict import predict
from airflow.operators.email import EmailOperator
from airflow.decorators import task
from operators.lotto_api_add_csv_operator import LottoApiAddCsvOperator
from airflow import Dataset
import pendulum

dataset_dags_dataset_producer = Dataset("dags_dataset_lotto_add")

with DAG(
    dag_id="dags_recommend_lottonum",
    schedule=[dataset_dags_dataset_producer],
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["lotto","recommend"]

) as dag:
    @task(task_id='inner_function1')
    def inner_func1(**kwargs):
        print('로또 번호 추천 작업 시작')

    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable=predict
            
    )
    send_email = EmailOperator(
            task_id='send_email',
            to='fresh0911@naver.com',
            subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
            html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 이번 주 추천 번호는 <br> \
            {{ti.xcom_pull(task_ids="python_t1")}} 입니다 <br>'
    )
            

    inner_func1() >>python_t1 >> send_email