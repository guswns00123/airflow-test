from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common.predict import predict
with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    #catchup 이 True일 경우 start_date로 정해놓은 시간이 1월 1일인데 내가 만약 3월 1일에 이 DAG을 실행한다면 3월 1일 이전에 밀린 모든 task를 실행 한다는 것
    catchup=False,
    # dagrun_timeout => 이 DAG가 일정시간 지나가면 멈추게하는 용도
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags => 단순 tags해주는 용도
    tags=["lotto","test"],
    # params task에 쓰일 것들 미리 선언해주는 정도
    # params={"example_key": "example_value"},
) as dag:
    
    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable=predict
        
    )
    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    #bash task 수행 순서 정해주기
    bash_t1 >>python_t1>> bash_t2
    # [END howto_operator_bash]