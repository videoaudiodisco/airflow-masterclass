from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    # decorator를 사용한 첫번째 그룹 지정
    @task_group(group_id='first_group')
    def group_1():
        '''task_group 데커레이터를 이용한 첫번째 그룹'''

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫번째 taskgroup 내 첫번째 task')
        
        inner_function2 = PythonOperator(
            task_id='inner_fucntion2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫번째 taskgroup 내 두번째 task'}
        )
    
        inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='두번째 그룹입니다') as group_2:
        '''여기에 적은 docstring은 표시 안됨'''
        @task(task_id='inner_function1') # task_id가 같지만, 다른 그룹에 속해있을때는 괜찮다. 
        def inner_func1(**kwargs):
            print('두번째 task group 내 첫번째 task')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'두번째 task group 내 두번째 task'}
        )

        inner_func1() >> inner_function2
    
    # group 도 이렇게 표현할 수 있다.
    group_1() >> group_2