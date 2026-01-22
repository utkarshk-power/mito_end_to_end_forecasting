import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import os
import datetime
from dotenv import load_dotenv
from dotenv import dotenv_values


load_dotenv()
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
repo_dir = os.getenv("REPO_DIR")
if not repo_dir:
    raise RuntimeError("REPO_DIR is not set")

raw_data_dir = os.path.join(repo_dir, "data", "raw", "data_without_temperature")

data_file = [f for f in os.listdir(raw_data_dir) if f.endswith(".csv")]
if not data_file:
    raise RuntimeError("No CSV files found in raw data directory")

site_data = pd.read_csv(os.path.join(raw_data_dir, data_file[0]))
site_data["_time"] = pd.to_datetime(site_data["_time"], utc=True)
start_date = site_data["_time"].iloc[0].date()
end_date = site_data["_time"].iloc[-1].date()

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 36.345,
	"longitude": 140.465,
	"start_date": start_date,
	"end_date": end_date,
	"hourly": "temperature_2m",
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["temperature_2m"] = hourly_temperature_2m

hourly_dataframe = pd.DataFrame(data = hourly_data)
print("\nHourly data\n", hourly_dataframe)

# Save the dataframe as a CSV file
hourly_dataframe.to_csv(os.path.join(repo_dir, "data", "raw", "only_temperature", "hourly_temperature_data.csv"), index=False)