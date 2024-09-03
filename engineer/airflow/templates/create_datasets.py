from airflow import DAG
from datetime import datetime
<replace_me> # import BigQueryCreateEmptyDatasetOperator class from bigquery_operator.py

DATASETS = [<replace_me>] # list of datasets created manually in activity 1 
PROJECT = "<replace_me>"  # your project id

default_args = {
    'owner': 'cl-student',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 0,
}

dag = DAG(
    "<replace_me>",  # name for dag
    default_args=<replace_me>, # argument dict variable
    schedule_interval=None,
)

for dataset in DATASETS:
    create_dataset_task = <replace_me> (  # operator class
        task_id=f'create_{dataset}',
        dataset_id=f'{dataset}_composer',
        project_id=PROJECT,
        dag=dag
    )