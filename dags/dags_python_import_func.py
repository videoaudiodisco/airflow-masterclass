from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp
import datetime
import pendulum

with DAG(
    dag_id="dags_python_import_func",
    schedule='0 8 * * *',
    start_date=pendulum.datetime(2023,3,1, tz="Asia/Seoul"),
    catchup=False
    ) as dag:

    task_get_sftp = PythonOperator(
        task_id = "tast_get_sftp",
        python_callable=get_sftp,
    )
