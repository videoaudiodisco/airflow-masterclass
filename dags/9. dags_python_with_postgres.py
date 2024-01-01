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

        print('lalala')

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            print('conn', conn)
            with closing(conn.cursor()) as cursor:
                print('cursor', cursor)
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                print('실행1')
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                print('실행2')
                conn.commit()
                print('실행3')

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5429', 'gypark', 'gypark', 'gypark']
    )
    print('실행4')
        
    insrt_postgres