import pandas as pd
import yaml
import openmeteo_requests
import requests_cache
from retry_requests import retry

# Load inference config
with open("inference.yaml", "r") as file:
    inference_params = yaml.safe_load(file)["temperature"]

def setup_openmeteo_client():
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

openmeteo = setup_openmeteo_client()

url = inference_params["api_url"]
params = {
    "latitude": inference_params["latitude"],
    "longitude": inference_params["longitude"],
    "hourly": inference_params["hourly"],
    "timezone": inference_params["timezone"],
    "forecast_days": inference_params["forecast_horizon_days"],
}

responses = openmeteo.weather_api(url, params=params)
response = responses[0]

hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

date_range_utc = pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)

hourly_data = {"date": date_range_utc}

hourly_data["temperature_2m"] = hourly_temperature_2m

os.makedirs(os.path.dirname(inference_params["output"]), exist_ok=True)
hourly_dataframe = pd.DataFrame(data = hourly_data)
print("\nHourly data\n", hourly_dataframe)
hourly_dataframe.to_csv(inference_params["output"])
