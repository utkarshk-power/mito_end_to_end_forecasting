from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import os

''' When retraining with new data, we are assuming that it will happen on central/dev side'''

REPO_DIR = os.getenv("REPO_DIR")
REMOTE_NAME = os.getenv("REMOTE_NAME")
MAIN_BRANCH = os.getenv("MAIN_BRANCH")
DATA_BRANCH = os.getenv("DATA_BRANCH")
LATEST_SHA = os.getenv("LATEST_SHA")

with DAG('retrain_if_new_data',
         start_date=datetime(2026,1,21),
         schedule=timedelta(minutes=5),
         catchup = False,
         default_args = { 'retries': 1, 'retry_delay': timedelta(minutes=5)},
         tags = ['retraining', 'new_data']) as dag:
    get_latest_sha = BashOperator(task_id='get_branch_sha',
                                    bash_command= "cd {{var.value.REPO_DIR}} && git fetch {{var.value.REMOTE_NAME}} "
                                                   "&& git rev-parse {{var.value.REMOTE_NAME}}/{{var.value.DATA_BRANCH}}",
                                    do_xcom_push=True)
    def should_retrain(**context):
        latest = context["ti"].xcom_pull(task_ids="get_branch_sha")
        prev = context["var"]["value"].get("LATEST_SHA", "")
        return latest != prev
    compare_latest_sha = ShortCircuitOperator(task_id='compare_latest_sha',
                                               python_callable=should_retrain)
    checkout_latest_sha = BashOperator(
      task_id='checkout_latest_sha',
      bash_command="cd {{ var.value.REPO_DIR }} && git checkout {{ ti.xcom_pull(task_ids='get_branch_sha') }}")
    pull_latest_data = BashOperator(task_id='pull_latest_data',
                                    bash_command = "cd {{var.value.REPO_DIR}} && dvc pull data/raw/data_without_temperature/mito_dataset_14march_2025_20jan_2026.csv")
    retrain_model = BashOperator(task_id='retrain_model_with_new_data',
                                                bash_command = "cd {{var.value.REPO_DIR}} && dvc repro")
    def save_latest_sha(**context):
        latest = context["ti"].xcom_pull(task_ids="get_branch_sha")
        Variable.set("LATEST_SHA", latest)

    save_sha = PythonOperator(
        task_id="save_latest_sha",
        python_callable=save_latest_sha,
      )

    get_latest_sha >> compare_latest_sha >> checkout_latest_sha >> pull_latest_data >> retrain_model >> save_sha



