from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg1',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    bash_upstream_1 = BashOperator(
        task_id = 'bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!') ### 여기에서 failure가 난다. 근데 trigger가 all_done 이므로 실행이 될 것

    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상처리')
    
    @task(task_id='python_downstream_1', trigger_rule='all_done') # task decorator 를 쓰는 경우에는 여기에 trigger rule 을 쓴다.
    def python_downstream_1():
        print('정상처리')
    
    # 만약 python operator를 쓴다면, 그 안에 들어가는 파라미터 중에 task_id, python_callable과 더불어 trigger_rule을 쓴다. 

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()