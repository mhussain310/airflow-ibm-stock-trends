from typing import List

import pandas as pd

from config.api_config import BASE_WEATHER_URL, get_weather_api_key
from config.file_path_config import BASE_RAW_DIR
from utils.file_utils import generate_data_file_path, save_dataframe_to_csv
from utils.request_utils import get_url


# Put the extracted data into a csv file and return path to file
def _extract_current_weather_data() -> str:
    # Fetch the current weather data
    current_weather_data = fetch_current_weather_data()

    # Extract the relavant data from the response
    parsed_current_weather_data = parse_current_weather_data(current_weather_data)

    # Convert the parsed data into a dataframe
    current_weather_df = pd.DataFrame([parsed_current_weather_data])

    # Save the dataframe as a CSV for logging purposes
    RAW_DATA_FILE_PATH = generate_data_file_path(
        prefix="extracted_current_weather", base_dir=BASE_RAW_DIR, subdir="weather"
    )
    save_dataframe_to_csv(current_weather_df, RAW_DATA_FILE_PATH)

    # Return the extracted dataframe
    return RAW_DATA_FILE_PATH


# Function to fetch current weather data from API
def fetch_current_weather_data(location: str = "New_York") -> dict:
    if not get_weather_api_key():
        raise ValueError("Missing WEATHER_API_KEY")

    url = f"{BASE_WEATHER_URL}/current.json?key={get_weather_api_key()}&q={location}&aqi=yes"
    return get_url(url).json()


# Function to extract relevant data from response
def parse_current_weather_data(response_data: dict) -> dict:
    location = response_data["location"]
    current = response_data["current"]
    condition = current["condition"]
    air_quality = current["air_quality"]

    return {
        "local_time": location["localtime"],
        "location_name": location["name"],
        "region": location["region"],
        "country": location["country"],
        "lat": location["lat"],
        "lon": location["lon"],
        "tz_id": location["tz_id"],
        "localtime_epoch": location["localtime_epoch"],
        "last_updated_epoch": current["last_updated_epoch"],
        "last_updated": current["last_updated"],
        "temp_c": current["temp_c"],
        "temp_f": current["temp_f"],
        "is_day": current["is_day"],
        "condition_text": condition["text"],
        "condition_icon": condition["icon"],
        "condition_code": condition["code"],
        "wind_mph": current["wind_mph"],
        "wind_kph": current["wind_kph"],
        "wind_degree": current["wind_degree"],
        "wind_dir": current["wind_dir"],
        "pressure_mb": current["pressure_mb"],
        "pressure_in": current["pressure_in"],
        "precip_mm": current["precip_mm"],
        "precip_in": current["precip_in"],
        "humidity": current["humidity"],
        "cloud": current["cloud"],
        "feelslike_c": current["feelslike_c"],
        "feelslike_f": current["feelslike_f"],
        "windchill_c": current["windchill_c"],
        "windchill_f": current["windchill_f"],
        "heatindex_c": current["heatindex_c"],
        "heatindex_f": current["heatindex_f"],
        "dewpoint_c": current["dewpoint_c"],
        "dewpoint_f": current["dewpoint_f"],
        "vis_km": current["vis_km"],
        "vis_miles": current["vis_miles"],
        "uv": current["uv"],
        "gust_mph": current["gust_mph"],
        "gust_kph": current["gust_kph"],
        "co": air_quality["co"],
        "no2": air_quality["no2"],
        "o3": air_quality["o3"],
        "so2": air_quality["so2"],
        "pm2_5": air_quality["pm2_5"],
        "pm10": air_quality["pm10"],
        "us_epa_index": air_quality["us-epa-index"],
        "gb_defra_index": air_quality["gb-defra-index"],
    }


if __name__ == "__main__":
    _extract_current_weather_data()

# for key, value in weather_record.items():
#     if value is None:
#         weather_record[key] = 0 if isinstance(value, (int, float)) else ""
