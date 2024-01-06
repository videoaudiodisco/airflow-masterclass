from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State 

with DAG(
    dag_id='dags_external_task_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:

    external_task_sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id='task_a',
        allowed_states=[State.SKIPPED], # task_a 가 skipped로 되면 sensor_a task는 success로 표시된다는 뜻
        execution_delta=timedelta(hours=6),
        poke_interval=10 # 10초
    )


    external_task_sensor_b = ExternalTaskSensor(
        task_id='external_task_sensor_b',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id='task_b',
        allowed_states=[State.SKIPPED], # task_b 가 skipped로 되면 sensor_b task는 success로 표시된다는 뜻
        execution_delta=timedelta(hours=6),
        poke_interval=10
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id='external_task_sensor_c',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id='task_c',
        failed_states=[State.SUCCESS], # task_c 가 success로 되면 sensor_c task는 failed로 표시된다는 뜻
        execution_delta=timedelta(hours=6),
        poke_interval=10
    )