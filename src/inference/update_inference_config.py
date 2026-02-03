import yaml
import datetime
import os

file_path = "inference.yaml"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} does not exist.")
with open(file_path, "r") as file:
    inference_params = yaml.safe_load(file)
    inference_temp_params = inference_params["preprocess_infer"]
    inference_lag_params = inference_params["lag_data"]


now_time = datetime.datetime.now(datetime.timezone.utc)
if now_time.minute > 0:
    now_time = now_time.replace(minute=0, second=0, microsecond=0)
inference_lag_params["lag_start_date"] = (now_time - datetime.timedelta(hours=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
inference_lag_params["lag_end_date"] = now_time.strftime('%Y-%m-%dT%H:%M:%SZ')
inference_temp_params["temp_start_date"] = now_time.strftime('%Y-%m-%dT%H:%M:%SZ')
inference_temp_params["temp_end_date"] = (now_time + datetime.timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%SZ')

with open(file_path, "w") as file:
    yaml.dump(inference_params, file, sort_keys=False)
print(f"Updated {file_path} with new inference time ranges.")

