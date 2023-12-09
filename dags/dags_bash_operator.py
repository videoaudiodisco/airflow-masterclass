from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
import datetime

import pendulum

# with DAG(
#     dag_id="my_producer",
#     schedule="0 0 * * *",
#     start_date=datetime(2023, 7, 1), 
#     catchup=False) as dag:

#     task_producer = BashOperator(
#         task_id="producer1",
#         bash_command='echo "hello world" >> /tmp/sample.txt',
#         outlets=[local_file])

with DAG(
    dag_id="dags_bash_operator",
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2021,1,1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),

    ) as dag:

    bash_t1 = BashOperator(
        task_id = "bash_t1",
        bash_command='echo whoami',
    )

    bash_t2 = BashOperator(
        task_id = "bash_t2",
        bash_command='echo %HOSTNAME',
    )

    bash_t1 >> bash_t2