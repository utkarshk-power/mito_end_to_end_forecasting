import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv


'''Read pulled data from dvc and temperature column to it'''

load_dotenv()
REPO_DIR = os.getenv("REPO_DIR")
data_folder = os.path.join(REPO_DIR, "data", "raw", "data_without_temperature")
temperature_data = pd.read_csv(os.path.join(REPO_DIR, "data", "raw", "only_temperature", "hourly_temperature_data.csv"))
'''Data without temperature column'''
for file in os.listdir(data_folder):
    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join(data_folder, file))

temperature_data.rename(columns={"date": "time"}, inplace=True)
temperature_data['time'] = pd.to_datetime(temperature_data['time'], utc=True)

'''Merge temperature data with net load data'''
df['_time'] = pd.to_datetime(df['_time'], utc=True)
df.rename(columns={'_time': 'time'}, inplace=True)
dataset = pd.merge(df, temperature_data, on="time", how='inner')

drop_cols = [c for c in ['_start', '_stop', 'table', 'result'] if c in dataset.columns]
dataset = dataset.drop(columns=drop_cols)
dataset.to_csv(f"{REPO_DIR}/data/raw/data_with_temperature/mito_dataset_14march_2025_20jan_2026.csv", index=False)

