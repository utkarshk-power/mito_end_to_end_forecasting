import mlflow
import os
import pandas as pd
import joblib
import yaml
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from urllib.parse import urlparse
import logging
from dotenv import dotenv_values, load_dotenv

logging.basicConfig(level=logging.INFO)
os.chdir("/Users/urkarsh.kulshrestha/Documents/AI_environment/work_env/appletree_end_to_end_forecasting")
with open("./params.yaml", "rb") as file:
    params = yaml.safe_load(file)['evaluate']

def evaluate_model(data_path, feature_cols, target_col, model_path, test_size, random_state):
    data=pd.read_csv(data_path)
    x= data[feature_cols]
    y = data[target_col]
    split_idx = int((1 - test_size) * len(data))
    x_test = x.iloc[split_idx:]
    y_test = y.iloc[split_idx:]
    model = joblib.load(model_path)
    predictions = model.predict(x_test)
    test_mse = mean_squared_error(y_test, predictions)
    test_mae = mean_absolute_error(y_test, predictions)
    test_r2 = r2_score(y_test, predictions)
    logging.info("Test Metrics: MSE=%.4f, MAE=%.4f, R2=%.4f", test_mse, test_mae, test_r2)


    with mlflow.start_run():
        mlflow.log_metrics({
            "test_mse": test_mse,
            "test_mae": test_mae,
            "test_r2": test_r2
        })
        mlflow.set_tag("model", "XGBoostRegressor")
        mlflow.set_tag("model_stage", "evaluate")
        mlflow.set_tag("Test Data Split", "15 %")
        mlflow.set_tag("Developer", "Utkarsh Kulshrestha")
        mlflow.set_tag("Data Used", "Mito Data March,2025-January,2026")

if __name__ == "__main__":
    load_dotenv("/Users/urkarsh.kulshrestha/Documents/AI_environment/work_env/appletree_end_to_end_forecasting/.env")
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Mito Load Forecasting with MLOps Principles")
    evaluate_model(params['data'], 
                   params['feature_cols'], 
                   params['target'], 
                   params['model_path'], 
                   params['test_size'], 
                   params['random_state'])