# Preprocessing code for inference temperature data

import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv
import pandas as pd
import yaml


logging.basicConfig(level=logging.INFO)

with open("inference.yaml", "r") as file:
    inference_all = yaml.safe_load(file)
    inference_params = inference_all["preprocess_infer"]


def load_temperature_forecast(path):
    temp_data = pd.read_csv(path)

    time_col = None
    for candidate in ["Datetime", "date", "time"]:
        if candidate in temp_data.columns:
            time_col = candidate
            break
    if time_col is None:
        raise ValueError("Temperature forecast must include a time column")

    temp_col = None
    for candidate in ["Average_Temperature_C", "Temperature_C", "temperature_2m"]:
        if candidate in temp_data.columns:
            temp_col = candidate
            break
    if temp_col is None:
        raise ValueError("Temperature forecast must include a temperature column")

    temp_data = temp_data.rename(columns={time_col: "time", temp_col: "temperature"})
    temp_data["time"] = pd.to_datetime(temp_data["time"], errors="coerce", utc=True)
    temp_data = temp_data.dropna(subset=["time"]).sort_values("time")

    start_date = inference_params.get("temp_start_date")
    end_date = inference_params.get("temp_end_date")
    if start_date and end_date:
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if start.tzinfo is None:
            start = start.tz_localize("UTC")
        else:
            start = start.tz_convert("UTC")
        if end.tzinfo is None:
            end = end.tz_localize("UTC")
        else:
            end = end.tz_convert("UTC")
        temp_data = temp_data[(temp_data["time"] >= start) & (temp_data["time"] <= end)]

    temp_data["temperature"] = temp_data["temperature"].astype(float)
    temp_data["time"] = temp_data["time"].dt.tz_localize(None)
    return temp_data[["time", "temperature"]]


if __name__ == "__main__":
    os.makedirs(os.path.dirname(inference_params["output"]), exist_ok=True)

    forecast = load_temperature_forecast(inference_params["input_temperature"])
    forecast.to_csv(inference_params["output"], index=False)
    logging.info("Inference input saved to: %s", inference_params["output"])

