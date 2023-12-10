from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import datetime

import pendulum

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule='10 0 * * 6#1',
    start_date=pendulum.datetime(2023,3,1, tz="Asia/Seoul"),
    catchup=False
    ) as dag:

    t1_orange = BashOperator(
        task_id = "t1_orange",
        bash_command='/opt/airflow/plugins/shell/select_fruit.sh ORANGE'
    )

    t2_apple = BashOperator(
        task_id = "t1_apple",
        bash_command='/opt/airflow/plugins/shell/select_fruit.sh APPLE'
    )


    t3_avocado = BashOperator(
        task_id = "t3_avocado",
        bash_command='/opt/airflow/plugins/shell/select_fruit.sh AVOCADO'
    )
    t1_orange >> t2_apple >> t3_avocado