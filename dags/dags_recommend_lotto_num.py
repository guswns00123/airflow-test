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
        task_id = 'predict_lotto_num',
        python_callable = predict_lotto_num
            
    )
    def select_postgres(postgres_conn_id, **kwargs):
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from contextlib import closing
            
            postgres_hook = PostgresHook(postgres_conn_id)
            with closing(postgres_hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    dag_id = kwargs.get('ti').dag_id
                    task_id = kwargs.get('ti').task_id
                    run_id = kwargs.get('ti').run_id
                    msg = 'hook select 수행'
                    sql = 'select * from lotto_add_table;'
                    cursor.execute(sql, (dag_id, task_id, run_id, msg))
                    rows =cursor.fetchall()
                    print(rows)
                    conn.commit()

    select_postgres_with_hook = PythonOperator(
            task_id='select_postgres_with_hook',
            python_callable=select_postgres,
            op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
        )
    send_num_to_email = EmailOperator(
            task_id='send_email',
            to='fresh0911@naver.com',
            subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
            html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 이번 주 추천 번호는 <br> \
            {{ti.xcom_pull(task_ids="python_t1")}} 입니다 <br>'
    )

    send_num_to_kakao = PythonOperator(
        task_id = 'send_num_to_kakao',
        python_callable = send_success_msg_to_kakao,
        
    )
            

    inner_func1() >> predict_lotto_num >> [send_num_to_email, send_num_to_kakao] 