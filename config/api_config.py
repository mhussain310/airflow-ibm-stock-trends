import os

# Weather API configurations
BASE_WEATHER_URL = "http://api.weatherapi.com/v1"


def get_weather_api_key():
    return os.getenv("WEATHER_API_KEY")


# Stock API configurations
def get_stock_api_key():
    return os.getenv("STOCK_API_KEY")
