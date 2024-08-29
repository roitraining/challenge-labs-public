from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


from datetime import datetime
import csv
import os
import logging
import table_ddl, view_ddl

TABLES = [
    'branches', 
    'customers', 
    'accounts', 
    'transactions',
    'credit_cards',
    'loans',
    'employees',
    'feedback',
    'card_transactions'
]

OPTIMIZED_TABLES = [
  "loans",
  "customer_info_nested",
  "transactions",
]

VIEWS = [
    "loans",
    "loans_by_month",
    "churn",
    "value",
    "upsell"
]

BUCKET = "your-project-id"
PROJECT = "your-project-id" 
RAW_DATASET = "raw_composer"
OPTIMIZED_DATASET = "optimized_composer"

def export_rows(table):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    select_sql = f'SELECT * FROM {table}'
    connection = pg_hook.get_conn()
    with connection.cursor('server_side_cursor') as cursor:
        cursor.execute(select_sql)
        tmp_file = f'/tmp/{table}.csv'
        with open(tmp_file, 'w') as f: 
            writer = csv.writer(f)
            while True:
                rows = cursor.fetchmany(1000)
                logging.info('exporting 1000')
                if not rows:
                    break
                writer.writerows(rows)
    
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_default')
    gcs_hook.upload(bucket_name=BUCKET,
                    object_name=f'db_export/{table}.csv',
                    filename=tmp_file)
    os.remove(tmp_file)

default_args = {
    'owner': 'sbcl',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 0,
}

dag = DAG(
    'prod_to_bq',
    default_args=default_args,
    description='Export, import, optimize, and create views',
    schedule_interval=None,
)

wait_for_exports = EmptyOperator(
    task_id='wait_for_exports',
    dag=dag
)

wait_for_imports = EmptyOperator(
    task_id='wait_for_imports',
    dag=dag
)

wait_for_optimized = EmptyOperator(
    task_id='wait_for_optimized',
    dag=dag
)

for table in TABLES:
    ex_branches = PythonOperator(
        task_id=f'export_{table}',
        python_callable=export_rows,
        op_kwargs={'table': table},
        dag=dag
    )
    ex_branches >> wait_for_exports

for table in TABLES:
    load_csv = GCSToBigQueryOperator(
        task_id=f'import_{table}',
        bucket=BUCKET,
        source_objects=[f'db_export/{table}.csv'],
        destination_project_dataset_table=f'{PROJECT}.{RAW_DATASET}.{table}',
        schema_object=f"schemas/{table}.json",
        write_disposition='WRITE_TRUNCATE',  # Overwrites the table if it exists
        source_format='CSV',
        dag=dag,
    )
    wait_for_exports >> load_csv >> wait_for_imports

for table in OPTIMIZED_TABLES:
    run_ddl = BigQueryExecuteQueryOperator(
        task_id=f'create_table_{table}',
        sql=table_ddl.ddls[table],
        use_legacy_sql=False,
        location='US',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )
    wait_for_imports >> run_ddl >> wait_for_optimized

for view in VIEWS:
    run_ddl = BigQueryExecuteQueryOperator(
        task_id=f'create_view_{view}',
        sql=view_ddl.ddls[view],
        use_legacy_sql=False,
        location='US',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )
    wait_for_optimized >> run_ddl