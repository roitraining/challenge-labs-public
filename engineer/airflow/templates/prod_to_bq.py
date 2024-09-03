from airflow import DAG
<replace me> # import PythonOperator class from operators.python_operator module
<replace me> # import PostgresHook class from hooks.postgres_hook module
<replace me> # import EmptyOperator class from operators.empty module
<replace me> # import GoogleCloudStorageHook class from contrib.hooks.gcs_hook module
<replace me> # import GCSToBigQueryOperator class from providers.google.cloud.transfers.gcs_to_bigquery module
<replace me> # import BigQueryExecuteQueryOperator class from providers.google.cloud.operators.bigquery module


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

BUCKET = "<replace_me>" # your-project-id
PROJECT = "<replace_me>" # your-project-id 
RAW_DATASET = "raw_composer"
OPTIMIZED_DATASET = "optimized_composer"

def export_rows(table):
    pg_hook = PostgresHook(postgres_conn_id='cloud_sql_postgres_via_proxy')
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
    'owner': 'cl-stduent',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 0,
}

dag = DAG(
    "replace_me" # dag name,
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
    ex_branches = <replace_me> (  # python operator
        task_id=f'export_{table}',
        python_callable=export_rows,
        op_kwargs={'table': table},
        dag=dag
    )
    <replace_me> # wait_for_exports depends on ex_branches

for table in TABLES:
    load_csv = <replace_me> (  # bq import operator
        task_id=f'import_{table}',
        bucket=BUCKET,
        source_objects=[f'db_export/{table}.csv'],
        destination_project_dataset_table=f'{PROJECT}.{RAW_DATASET}.{table}',
        schema_object=f"schemas/{table}.json",
        write_disposition='WRITE_TRUNCATE',  # Overwrites the table if it exists
        source_format='CSV',
        dag=dag,
    )
    <replace_me> # load_csv depends on wait_for_exports and wait_for_imports depends on load_csv

for table in OPTIMIZED_TABLES:
    run_ddl = <replace_me> ( # bq execute query operator
        task_id=f'create_table_{table}',
        sql=table_ddl.ddls[table],
        use_legacy_sql=False,
        location='US',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )
    <replace_me> # load_csv depends on wait_for_imports and wait_for_optimized depends on run_ddl

for view in VIEWS:
    run_ddl = <replace_me> (  # bq execute query operator
        task_id=f'create_view_{view}',
        sql=view_ddl.ddls[view],
        use_legacy_sql=False,
        location='US',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )
    <replace_me> # run_ddl depends on wait_for_optimized