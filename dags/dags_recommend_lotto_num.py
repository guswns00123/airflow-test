from airflow import DAG
from airflow.operators.python import PythonOperator
from common.predict import predict_lotto_num
from airflow.operators.email import EmailOperator
from airflow.decorators import task
from airflow import Dataset
from datetime import timedelta
import pendulum
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao
from config.send_msg_to_kakao import send_success_msg_to_kakao


dataset_dags_dataset_producer = Dataset("dags_lotto_data")

with DAG(
    dag_id="dags_recommend_lotto_num",
    schedule=[dataset_dags_dataset_producer],
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["lotto","recommend"],
    default_args={
        'on_failure_callback':on_failure_callback_to_kakao,
        'execution_timeout': timedelta(seconds=180)
    }

) as dag:
    @task(task_id='inner_function1')
    def inner_func1(**kwargs):
        print('로또 번호 추천 작업 시작')

    predict_lotto_num = PythonOperator(
        task_id = 'python_t1',
        python_callable = predict_lotto_num
            
    )

    send_num_to_email = EmailOperator(
            task_id='send_email',
            to='fresh0911@naver.com',
            subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
            html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 이번 주 추천 번호는 <br> \
            {{ti.xcom_pull(task_ids="python_t1")}} 입니다 <br>'
    )

    send_num_to_kakao = PythonOperator(
        task_id = 'python_t2',
        python_callable = send_success_msg_to_kakao,
        
    )
            

    inner_func1() >> predict_lotto_num >> [send_num_to_email, send_num_to_kakao] 