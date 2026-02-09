## Task: Push the data periodically from edge device to DVC via systemd based scheduler

import time
import schedule
import influxdb_client
import pandas as pd
import os
import dotenv
import yaml
import datetime
import subprocess

dotenv.load_dotenv()
with open("inference.yaml") as file:
    config = yaml.safe_load(file)
with open("state.yaml") as file:
    state = yaml.safe_load(file)

def read_data_from_influx():
    # Function to read data from InfluxDB
    client = influxdb_client.InfluxDBClient(url=os.getenv("INFLUX_URL"), token=os.getenv("INFLUX_TOKEN"), org=os.getenv("INFLUX_ORG"))
    query_api = client.query_api()
    measurement = config.get("new_data_params", {}).get("measurement", "")
    fields = config.get("new_data_params", {}).get("fields", "actualNetLoad")
    fields_filter = f'r["_field"] == "{fields}"'
    query = f'''
    from(bucket: "{os.getenv("INFLUX_BUCKET_NAME")}")
    |> range(start:-15d)
    |> filter(fn: (r) => 
        r["_measurement"] == "{measurement}"
        )
    |> filter(fn: (r) => {fields_filter})
    |> aggregateWindow(every: 60m, fn: mean, createEmpty: false)
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    result = query_api.query_data_frame(query=query, org=os.getenv("INFLUX_ORG"))
    client.close()
    if isinstance(result, list):
        result = pd.concat(result, ignore_index=True)
    return result

def save_data_as_csv(data):
    df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    filename=config.get("new_data_params", {}).get("output_file", "latest_site_data.csv")
    df.to_csv(filename, index=False)

def push_data_to_dvc(data):
    current_max_timestamp = data["_time"].max() if not data.empty else None
    current_max_timestamp_dt = pd.to_datetime(current_max_timestamp) if current_max_timestamp else None
    last_max_timestamp = state.get("data_timestamp", {}).get("latest", None)
    last_max_timestamp_dt = pd.to_datetime(last_max_timestamp) if last_max_timestamp else None
    if last_max_timestamp_dt==None or current_max_timestamp_dt > last_max_timestamp_dt:
        update_state = True
    else:
        update_state = False

    if update_state:
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))) 
        output_file = config.get("new_data_params", {}).get("output_file", "latest_site_data.csv")
        subprocess.run(["git", "checkout", "data-snapshots"], cwd=repo_root, check=True)
        subprocess.run(["git", "pull", "--ff-only", "origin", "data-snapshots"], cwd=repo_root, check=True)
        subprocess.run(["dvc", "add", output_file], cwd=repo_root, check=True)
        subprocess.run(["git", "add", f"{output_file}.dvc"], cwd=repo_root, check=True)
        subprocess.run(["git", "commit", "-m", f"Data Snapshot {datetime.datetime.now()-datetime.timedelta(days=15)}_{datetime.datetime.now()}"], 
                   cwd=repo_root, check=True)
        subprocess.run(["dvc", "push"], cwd=repo_root, check=True)
        subprocess.run(["git", "push", "origin", "data-snapshots"], cwd=repo_root, check=True)
        state["data_timestamp"]["latest"] = current_max_timestamp_dt.isoformat()
        with open("state.yaml", "w") as file:
            yaml.dump(state, file)

def scheduled_tasks():
    print("Reading New Data from site database and saving as .csv file")
    data = read_data_from_influx()
    if data is None or data.empty:
        print("No new data fetched from InfluxDB.")
        return
    save_data_as_csv(data)
    push_data_to_dvc(data)

if __name__ == "__main__":
    scheduled_tasks()