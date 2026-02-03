import mlflow
import urllib
from urllib.parse import urlparse
import os
import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split, GridSearchCV
import joblib
import yaml
import logging
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from dotenv import dotenv_values, load_dotenv


logging.basicConfig(level=logging.INFO)
load_dotenv()
REPO_DIR = os.getenv("REPO_DIR")
if not REPO_DIR:
    raise RuntimeError("REPO_DIR is not set")

with open(os.path.join(REPO_DIR, "params.yaml"), "rb") as file:
    params = yaml.safe_load(file)['train'] 

def prepare_train_test_data(input_path, feature_cols, target, train_size, val_size, random_state):
    data=pd.read_csv(input_path)
    x= data[feature_cols]
    y= data[target]
    split_idx = int(len(data) * train_size)
    split_idx_val = split_idx + int(len(data) * val_size)
    x_train, x_eval = x[0:split_idx], x[split_idx: split_idx_val]
    y_train, y_eval = y[0:split_idx], y[split_idx: split_idx_val]
    logging.info("Training data size: %d, Evaluation data size: %d", len(x_train), len(x_eval))
    return x_train, x_eval, y_train, y_eval

def train_model(x_train, y_train, n_estimators, max_depth, learning_rate, subsample):
    tscv = TimeSeriesSplit(n_splits=3)
    param_grid = {
        'n_estimators': n_estimators,
        'max_depth': max_depth,
        'learning_rate': learning_rate,
        'subsample': subsample
    }
    grid_search = GridSearchCV(estimator=XGBRegressor(),
                               param_grid=param_grid,
                               cv=tscv,
                               scoring='neg_mean_squared_error'
    )
    grid_search.fit(x_train,y_train)
    best_model = grid_search.best_estimator_
    logging.info("Best model parameters: %s", grid_search.best_params_)
    return best_model, grid_search.best_params_

if __name__ == "__main__":
    load_dotenv(os.path.join(REPO_DIR, ".env"))
    logging.basicConfig(level=logging.INFO)
    print(os.environ.get("MLFLOW_TRACKING_URI"))
    print(os.environ.get("MLFLOW_TRACKING_USERNAME"))
    print("token set:", bool(os.environ.get("MLFLOW_TRACKING_PASSWORD")))
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Mito Load Forecasting with MLOps Principles")
    tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme
    x_train, x_eval, y_train, y_eval = prepare_train_test_data(params['data'], 
                                                             params['feature_cols'], 
                                                             params['target'], 
                                                             params['train_size'], 
                                                             params['val_size'],
                                                             params['random_state'])
    model, best_params = train_model(x_train, y_train, params['n_estimators'], 
                                    params['max_depth'], params['learning_rate'], 
                                    params['subsample'])
    train_predictions = model.predict(x_train)
    val_predictions = model.predict(x_eval)
    train_mse=mean_squared_error(y_train, train_predictions)
    train_mae=mean_absolute_error(y_train, train_predictions)
    train_r2=r2_score(y_train, train_predictions)
    val_mse=mean_squared_error(y_eval, val_predictions)
    val_mae=mean_absolute_error(y_eval, val_predictions)
    val_r2=r2_score(y_eval, val_predictions)
    logging.info("Training Metrics: MSE=%.4f, MAE=%.4f, R2=%.4f", train_mse, train_mae, train_r2)
    logging.info("Validation Metrics: MSE=%.4f, MAE=%.4f, R2=%.4f", val_mse, val_mae, val_r2)
    with mlflow.start_run():
        mlflow.log_params(best_params)
        mlflow.log_metrics({"train_mse": train_mse, "train_mae": train_mae, "train_r2": train_r2,
                             "val_mse": val_mse, "val_mae": val_mae, "val_r2": val_r2})
        mlflow.set_tag("model_type", "XGBoost Regressor")
        mlflow.set_tag("Developer", "Utkarsh Kulshrestha")
        mlflow.set_tag("Data Used", " Mito Data March,2025-January,2026")
        mlflow.set_tag("Site", "Apple Tree - Mito")
        signature = mlflow.models.infer_signature(x_train, train_predictions)
        if tracking_url_type_store != "file":
            mlflow.sklearn.log_model(sk_model=model, artifact_path="model", signature=signature, input_example=x_train.head(3))
    os.makedirs(os.path.dirname(params['model_path']), exist_ok=True)
    joblib.dump(model, params['model_path'])
    logging.info("Model saved at %s", params['model_path'])
