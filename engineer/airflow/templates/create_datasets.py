from airflow import DAG
from datetime import datetime
# insert code to import BigQueryCreateEmptyDatasetOperator class from bigquery_operator.py

DATASETS = #list of datasets created manually in activity 1 
PROJECT = #your project id

default_args = {
    'owner': 'sbcl',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 0,
}

dag = DAG(
    #name for dag,
    default_args=#argument dict,
    schedule_interval=None,
)

for dataset in DATASETS:
    create_dataset_task = #operator class(
        task_id=f'create_{dataset}',
        dataset_id=f'{dataset}_composer',
        project_id=PROJECT,
        dag=dag
    )