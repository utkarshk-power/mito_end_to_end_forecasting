import pandas as pd
import yaml
import os
import numpy as np
import logging
from dotenv import load_dotenv



logging.basicConfig(level=logging.INFO)

load_dotenv()
REPO_DIR = os.getenv("REPO_DIR")
## Load the parameters and file paths from param.yaml
with open(os.path.join(REPO_DIR, "params.yaml"), "r") as file:
    params = yaml.safe_load(file)['preprocess']

'''This function will load input data and convert netload into rolling lag values'''
def preprocess_(input_path):
    input_data=pd.read_csv(input_path)
    required_cols = {'time', '_measurement', 'actualNetLoad', 'temperature_2m'}
    missing = required_cols - set(input_data.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    input_data = input_data.sort_values(by='time')
    input_data.rename(columns={'actualNetLoad': 'netload', 'temperature_2m': 'temperature'}, inplace=True)
    logging.info("Length of original data: %d", len(input_data))
    netload = input_data['netload'].tolist()
    temperature = input_data['temperature'].tolist()
    return (input_data)

def create_training_features(input_df, lag_time):
    input_df['time'] = input_df['time'].astype(str)
    input_df['Datetime'] = pd.to_datetime(input_df['time'])
    input_df['Hour'] = input_df['Datetime'].dt.hour
    input_df['Day_of_Week'] = input_df['Datetime'].dt.dayofweek
    input_df['Month'] = input_df['Datetime'].dt.month
    input_df['DayOfYear'] = input_df['Datetime'].dt.dayofyear
    for i in range(1, lag_time + 1):
        input_df[f"netload_lag{i}"] = input_df['netload'].shift(i)
    lag_cols = [f"netload_lag{i}" for i in range(1, lag_time + 1)]
    return input_df.dropna(subset=lag_cols)

if __name__ == "__main__":
    input_data= preprocess_(params['input_path'])
    processed_dataframe = create_training_features(input_data, params['lag_time'])
    print(processed_dataframe.head())
    processed_dataframe.to_csv(params['output_path'])
    print("Preprocessing complete. Processed data saved to:", params['output_path'])
    logging.info("Preprocessing complete. Processed data saved to: %s", params['output_path'])