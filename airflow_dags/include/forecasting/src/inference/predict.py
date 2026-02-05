# This code reads the inference input and iteratively predicts future load.

import logging
import os
import re
from glob import glob
import joblib
import pandas as pd
import yaml
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
bucket_name = os.getenv("INFLUX_BUCKET_NAME")
api_token = os.getenv("INFLUX_TOKEN")
url = os.getenv("INFLUX_URL")
org = os.getenv("INFLUX_ORG")

with open("inference.yaml", "r") as file:
    inference_params = yaml.safe_load(file)

with open("params.yaml", "r") as file:
    train_params = yaml.safe_load(file)["train"]


def load_lag_values(path, lag_time):
    data = pd.read_csv(path)

    if "_time" in data.columns:
        data = data.rename(columns={"_time": "time"})
    
    data["time"] = pd.to_datetime(data["time"])
    data = data.sort_values("time")

    if "actualNetLoad" in data.columns:
        data=data.rename(columns={"actualNetLoad": "netload"})
    
    netload = data["netload"].astype(float).tolist()
    
    if len(netload) < lag_time:
        raise ValueError(f"Need at least {lag_time} rows for lag features")
    return netload[-lag_time:]


def load_forecast_input(path):
    data = pd.read_csv(path)

    if "time" not in data.columns:
        raise ValueError("Inference input must include a time column")

    if "Average_Temperature_C" not in data.columns:
        temp_col = None
        for candidate in ["Temperature_C", "temperature_2m", "temperature"]:
            if candidate in data.columns:
                temp_col = candidate
                break
        if temp_col is None:
            raise ValueError("Inference input must include temperature column")
        data = data.rename(columns={temp_col: "temperature"})

    data["time"] = pd.to_datetime(data["time"], errors="coerce")
    data = data.dropna(subset=["time"]).sort_values("time")
    data["temperature"] = data["temperature"].astype(float)
    return data[["time", "temperature"]]


def build_feature_row(timestamp, temperature, lag_state):
    features = {
        "Hour": timestamp.hour,
        "temperature": temperature,
        "Day_of_Week": timestamp.dayofweek,
        "Month": timestamp.month,
        "DayOfYear": timestamp.dayofyear,
    }
    for i in range(1, len(lag_state) + 1):
        features[f"netload_lag{i}"] = lag_state[-i]
    return features

def write_to_influx_db(predictions, bucket_name, org, api_token, url):
    client = influxdb_client.InfluxDBClient(url=url, token=api_token, org=org)
    write_api = client.write_api()
    ## Using point object for type safety
    for _, row in predictions.iterrows():
        point = (
            influxdb_client.Point("inference_predictions")
            .tag("location", "Tokyo")
            .field("predicted_netload", row["predicted_netload"])
            .time(row["time"])
        )
        
        write_api.write(bucket=bucket_name, record=point)

    client.close()


def iterative_predict(model, forecast_data, lag_state, feature_cols):
    predictions = []
    lag_state = list(lag_state)

    for _, row in forecast_data.iterrows():
        feature_row = build_feature_row(
            row["time"], row["temperature"], lag_state
        )
        feature_frame = pd.DataFrame([feature_row])[feature_cols]
        predicted = float(model.predict(feature_frame)[0])

        predictions.append(
            {
                "time": row["time"],
                "temperature": row["temperature"],
                "predicted_netload": predicted,
                **{f"netload_lag{i}": lag_state[-i] for i in range(1, len(lag_state) + 1)},
            }
        )
        lag_state.append(predicted)
        lag_state = lag_state[1:]

    return pd.DataFrame(predictions)


def make_predictions():
    preprocess_cfg = inference_params["preprocess_infer"]
    predict_cfg = inference_params["predict"]
    lag_time = preprocess_cfg["lag_time"]

    model = joblib.load(predict_cfg["model"])
    lag_state = load_lag_values(preprocess_cfg["input_load"], lag_time)
    forecast_data = load_forecast_input(predict_cfg["input"])
    feature_cols = preprocess_cfg.get("cols_needed", train_params["feature_cols"])
    predictions = iterative_predict(model, forecast_data, lag_state, feature_cols)
    if "time" in predictions.columns:
        times = pd.to_datetime(predictions["time"], errors="coerce")
        if times.dt.tz is None:
            times = times.dt.tz_localize("UTC")
        predictions["time"] = times.dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
    predict_time_start = inference_params["preprocess_infer"]["temp_start_date"]
    predict_time_start = predict_time_start.replace(":", "-")
    predict_time_start = predict_time_start.replace(" ", "_")
    base_path = predict_cfg["output"] + predict_time_start
    output_path = base_path + ".csv"
    os.makedirs(os.path.dirname(base_path), exist_ok=True)

    if os.path.exists(output_path):
        existing = glob(f"{base_path}*.csv")
        pattern = re.compile(rf"^{re.escape(base_path)}_(\d+)\.csv$")
        max_suffix = 0
        for path in existing:
            match = pattern.match(path)
            if match:
                max_suffix = max(max_suffix, int(match.group(1)))
        output_path = f"{base_path}_{max_suffix + 1}.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    write_to_influx_db(predictions, bucket_name, org, api_token, url)
    predictions.to_csv(output_path, index=False)
    logging.info("Predictions saved to: %s", output_path)


if __name__ == "__main__":
    make_predictions()
