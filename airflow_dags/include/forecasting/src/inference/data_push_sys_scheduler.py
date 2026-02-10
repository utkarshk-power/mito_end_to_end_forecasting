import time
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
       # Read from state.yaml (ISO string with timezone)
    start_iso = state.get("data_timestamp", {}).get("last_pushed")
    if not start_iso:
        # safety fallback if state missing
        start_iso = "2026-01-13T12:00:00+00:00"
    start_iso = pd.to_datetime(start_iso, utc=True).isoformat()
    # Flux wants a time value. time(v: "...") is reliable for ISO strings.
    query = f'''
    from(bucket: "{os.getenv("INFLUX_BUCKET_NAME")}")
    |> range(start: time(v: "{start_iso}"), stop: now())
    |> filter(fn: (r) => r["_measurement"] == "{measurement}")
    |> filter(fn: (r) => r["_field"] == "{fields}")
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
    os.makedirs(os.path.dirname(config.get("new_data_params", {}).get("output_file", "latest_site_data.csv")), exist_ok=True)
    filename=config.get("new_data_params", {}).get("output_file", "latest_site_data.csv")
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename} on {pd.Timestamp.now(tz='UTC').isoformat()}")

def push_data_to_dvc(data):
    if data is None or data.empty:
        print("No data to push.")
        return
    now = pd.Timestamp.now(tz="UTC")
    current_max_timestamp = data["_time"].max()
    current_max_timestamp_dt = pd.to_datetime(current_max_timestamp) if current_max_timestamp else None
    last_push_timestamp = state.get("data_timestamp", {}).get("last_pushed", None)
    last_push_timestamp_dt = pd.to_datetime(last_push_timestamp) if last_push_timestamp else None

    new_data_available = (last_push_timestamp_dt is None) or (current_max_timestamp_dt > last_push_timestamp_dt)
    enough_time_passed = (last_push_timestamp_dt is None) or (now - last_push_timestamp_dt >= pd.Timedelta(days=1))

    update_state = new_data_available and enough_time_passed

    if update_state:
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))) 
        #repo_root = os.path.dirname(os.path.abspath(__file__))
        output_file = config.get("new_data_params", {}).get("output_file", "latest_site_data.csv")
        subprocess.run(["git", "checkout", "data-snapshots"], cwd=repo_root, check=True)
        subprocess.run(["git", "pull", "--ff-only", "origin", "data-snapshots"], cwd=repo_root, check=True)
        subprocess.run(["dvc", "add", output_file], cwd=repo_root, check=True)
        subprocess.run(["git", "add", f"{output_file}.dvc"], cwd=repo_root, check=True)
        subprocess.run(["git", "commit", "-m", f"Data Snapshot {current_max_timestamp_dt.isoformat()}"], 
                   cwd=repo_root, check=True)
        subprocess.run(["dvc", "push"], cwd=repo_root, check=True)
        subprocess.run(["git", "push", "origin", "data-snapshots"], cwd=repo_root, check=True)
        state["data_timestamp"]["last_pushed"] = current_max_timestamp_dt.isoformat()
        with open("state.yaml", "w") as file:
            yaml.dump(state, file)
    else:
        print("No new data to push to DVC.")
        subprocess.run(["git", "checkout", "main"], cwd=repo_root, check=True)

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