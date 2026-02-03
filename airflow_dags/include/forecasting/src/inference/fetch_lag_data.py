# We will be reading using lag data of previous 5 time steps from our database

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import yaml
import os
from dotenv import load_dotenv

load_dotenv()

# Load YAML parameters
with open("inference.yaml", "r") as file:
    inference_all = yaml.safe_load(file)
    inference_params = inference_all["lag_data"]

bucket_name = os.getenv("INFLUX_BUCKET_NAME")
api_token = os.getenv("INFLUX_TOKEN")
url = os.getenv("INFLUX_URL")
org = os.getenv("INFLUX_ORG")
lag_start = inference_params["lag_start_date"].replace(" ", "T")
lag_end = inference_params["lag_end_date"].replace(" ", "T")
'''
if not lag_start.endswith("Z"):
    lag_start += "Z"
if not lag_end.endswith("Z"):
    lag_end += "Z"
'''
client = influxdb_client.InfluxDBClient(url=url, token=api_token, org=org)
query_api = client.query_api()
fields = inference_params["load_fields"]

fields_filter = " or ".join([f'r["_field"] == "{field}"' for field in fields])

query = f'''
from(bucket: "{bucket_name}")
  |> range(start: time(v: "{lag_start}"), stop: time(v: "{lag_end}"))
  |> filter(fn: (r) => 
      r["_measurement"] == "FncPvSc" 
      )
  |> filter(fn: (r) => {fields_filter})
  |> aggregateWindow(every: 60m, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
'''


site_data = query_api.query(query=query, org=org)
site_data_df = pd.DataFrame([record.values for table in site_data for record in table.records])
os.makedirs(os.path.dirname(inference_params["output"]), exist_ok=True)
site_data_df = site_data_df.drop(columns=['_start', '_stop', 'table', 'result'])
print(site_data_df)
site_data_df.to_csv(inference_params["output"])
