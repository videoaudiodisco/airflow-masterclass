# from airflow import DAG
# import pendulum
# from airflow.operators.python import PythonOperator

# with DAG(
#     dag_id='dags_python_with_postgres',
#     start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
#     schedule=None,
#     catchup=False
# ) as dag:

#     def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
#         import psycopg2
#         from contextlib import closing

#         with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
#             with closing(conn.cursor()) as cursor:
#                 dag_id = kwargs.get('ti').dag_id
#                 task_id = kwargs.get('ti').task_id
#                 run_id = kwargs.get('ti').run_id
#                 msg = 'insrt 수행'
#                 sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s)'
#                 cursor.execute(sql, (dag_id, task_id, run_id, msg))
#                 conn.commit()
    
#     insrt_postgres = PythonOperator(
#         task_id = 'insrt_postgres',
#         python_callable=insrt_postgres,
#         op_args=['172.28.0.3', '5429', 'gypark', 'gypark', 'gypark']
#     )

#     insrt_postgres


from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,

        # 그렇다면 외부에서(local 컴퓨터에서) 접속할 때는 5429 포트로 접속하시는게 맞습니다.
        # 하지만 컨테이너들끼리 접속할 때에는 5432로 접속하셔야 해요 .
        # 5429는 컨테이너가 외부로 노출할 때의 IP이고 컨테이너들끼리 통신할때는 내부 포트인 5432를 사용합니다.
        op_args=['172.28.0.3', '5432', 'gypark', 'gypark', 'gypark']

        ## daily_code 용
        # op_args=['172.21.0.2', '5555', 'daily_code', 'postgres', 'password1234']

    )
        
    insrt_postgres