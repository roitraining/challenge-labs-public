from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

def test():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    select_sql = f'SELECT * FROM branches'
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(select_sql)

default_args = {
    'owner': 'sbcl',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 0,
}

dag = DAG(
    'proxy_test',
    default_args=default_args,
    description='Make sure Cloud Proxy works',
    schedule_interval=None,
)

test = PythonOperator(
    task_id=f'test',
    python_callable=test,
    dag=dag
)

test