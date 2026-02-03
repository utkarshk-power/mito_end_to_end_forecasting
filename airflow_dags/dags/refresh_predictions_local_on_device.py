from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os


with DAG('refresh_lag_data_and_prediction_dag',
         start_date=datetime(2026,2,3),
         schedule=timedelta(hours=5),
         catchup=False,
         default_args = { 'retries': 1, 'retry_delay': timedelta(minutes=5)},
         tags = ['forecasting', 'inference', 'docker', 'appletree_mito'],
         ) as dag:
    task_env = {
        "INFLUX_BUCKET_NAME": os.getenv("INFLUX_BUCKET_NAME", ""),
        "INFLUX_TOKEN": os.getenv("INFLUX_TOKEN", ""),
        "INFLUX_URL": os.getenv("INFLUX_URL", ""),
        "INFLUX_ORG": os.getenv("INFLUX_ORG", ""),
    }
    
    prepare_dirs = BashOperator(
        task_id="prepare_dirs",
        bash_command="mkdir -p /usr/local/airflow/include/forecasting/data/inference/raw/only_temperature /usr/local/airflow/include/forecasting/data/inference/raw/data_without_temperature /usr/local/airflow/include/forecasting/data/inference/raw/data_with_temperature /usr/local/airflow/include/forecasting/data/inference/predictions",
        env=task_env,
    )
    update_inference_config = BashOperator(
        task_id="update_inference_config",
        bash_command="python src/inference/update_inference_config.py",
        cwd="/usr/local/airflow/include/forecasting",
        env=task_env,
    )
    fetch_lag_data = BashOperator(
        task_id="fetch_lag_data",
        bash_command="python src/inference/fetch_lag_data.py",
        cwd="/usr/local/airflow/include/forecasting",
        env=task_env,
    )
    fetch_weather = BashOperator(
        task_id="fetch_weather_data",
        bash_command="python src/inference/fetch_weather_inference.py",
        cwd="/usr/local/airflow/include/forecasting",
        env=task_env,
    )
    preprocess_infer = BashOperator(
        task_id="preprocess_inference",
        bash_command="python src/inference/preprocess_inference.py",
        cwd="/usr/local/airflow/include/forecasting",
        env=task_env,
    )
    predict = BashOperator(
        task_id="predict",
        bash_command="python src/inference/predict.py",
        cwd="/usr/local/airflow/include/forecasting",
        env=task_env,
    )

    prepare_dirs >> update_inference_config >> fetch_lag_data >> fetch_weather >> preprocess_infer >> predict
