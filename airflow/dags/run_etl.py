from airflow.decorators import dag, task, task_group

from etl.extract.extract_current_weather import _extract_current_weather_data
from etl.extract.extract_historical_weather import _extract_historical_weather_data
from etl.extract.extract_stock_data import _extract_stock_data
from etl.load.load import _load_data
from etl.transform.clean_stock_data import _clean_stock_data
from etl.transform.clean_weather_data import _clean_weather_data
from etl.transform.enrich_stock_data import _enrich_stock_data
from etl.transform.enrich_weather_data import _enrich_weather_data
from etl.transform.merge_data import _merge_data
from utils.file_utils import _clear_data_folders


@dag
def run_etl():
    @task
    def clear_data_folders():
        print("Removing temp files from previous run...")
        _clear_data_folders()
        print("Removal of temp files complete.")

    @task_group
    def extract_data():
        @task
        def extract_current_weather_data():
            return _extract_current_weather_data()

        @task(multiple_outputs=True)
        def extract_historical_weather_data():
            return _extract_historical_weather_data()

        @task(multiple_outputs=True)
        def extract_stock_data():
            return _extract_stock_data()

        # Instantiate tasks
        current_weather = extract_current_weather_data()
        historical_weather = extract_historical_weather_data()
        stock = extract_stock_data()

        # Return dictionary of results
        return {
            "current_weather": current_weather,
            "hourly_historical_weather": historical_weather["hourly"],
            "daily_historical_weather": historical_weather["daily"],
            "hourly_stock": stock["hourly"],
            "daily_stock": stock["daily"],
        }

    @task_group
    def clean_data(extracted: dict):
        @task
        def clean_current_weather_data(file_path):
            return _clean_weather_data(
                input_file_path=file_path,
                output_file_name="cleaned_current_weather",
                sort_by="local_time",
            )

        @task
        def clean_hourly_historical_weather_data(file_path):
            return _clean_weather_data(
                input_file_path=file_path,
                output_file_name="cleaned_hourly_historical",
                sort_by="time",
            )

        @task
        def clean_daily_historical_weather_data(file_path):
            return _clean_weather_data(
                input_file_path=file_path,
                output_file_name="cleaned_daily_historical",
                sort_by="date",
            )

        @task
        def clean_hourly_stock_data(file_path):
            return _clean_stock_data(
                input_file_path=file_path,
                output_file_name="cleaned_hourly_stock_data",
                sort_by="timestamp",
                has_time=True,
            )

        @task
        def clean_daily_stock_data(file_path):
            return _clean_stock_data(
                input_file_path=file_path,
                output_file_name="cleaned_daily_stock_data",
                sort_by="timestamp",
            )

        cleaned_current_weather = clean_current_weather_data(
            extracted["current_weather"]
        )
        cleaned_hourly_historical_weather = clean_hourly_historical_weather_data(
            extracted["hourly_historical_weather"]
        )
        cleaned_daily_historical_weather = clean_daily_historical_weather_data(
            extracted["daily_historical_weather"]
        )
        cleaned_hourly_stock = clean_hourly_stock_data(extracted["hourly_stock"])
        cleaned_daily_stock = clean_daily_stock_data(extracted["daily_stock"])

        return {
            "current_weather": cleaned_current_weather,
            "hourly_historical_weather": cleaned_hourly_historical_weather,
            "daily_historical_weather": cleaned_daily_historical_weather,
            "hourly_stock": cleaned_hourly_stock,
            "daily_stock": cleaned_daily_stock,
        }

    @task_group
    def enrich_data(cleaned: dict):
        @task
        def enrich_hourly_historical_weather_data(file_path):
            return _enrich_weather_data(
                input_file_path=file_path,
                output_file_name="enriched_hourly_historical",
                column="time",
            )

        @task
        def enrich_hourly_stock_data(file_path):
            return _enrich_stock_data(
                input_file_path=file_path,
                output_file_name="enriched_hourly_stock_data",
                column="timestamp",
            )

        enriched_hourly_historical_weather = enrich_hourly_historical_weather_data(
            cleaned["hourly_historical_weather"]
        )
        enriched_hourly_stock = enrich_hourly_stock_data(cleaned["hourly_stock"])

        return {
            "hourly_historical_weather": enriched_hourly_historical_weather,
            "hourly_stock": enriched_hourly_stock,
        }

    @task_group
    def merge_data(cleaned: dict, enriched: dict):
        @task
        def merge_hourly_stock_and_weather_data(file_path_1, file_path_2):
            return _merge_data(
                input_file_path_1=file_path_1,
                input_file_path_2=file_path_2,
                left_on="time",
                right_on="timestamp",
                output_file_name="merged_hourly_data",
            )

        @task
        def merge_daily_stock_and_weather_data(file_path_1, file_path_2):
            return _merge_data(
                input_file_path_1=file_path_1,
                input_file_path_2=file_path_2,
                left_on="date",
                right_on="timestamp",
                output_file_name="merged_daily_data",
            )

        merged_hourly_stock_and_weather = merge_hourly_stock_and_weather_data(
            enriched["hourly_historical_weather"], enriched["hourly_stock"]
        )
        merged_daily_stock_and_weather = merge_daily_stock_and_weather_data(
            cleaned["daily_historical_weather"], cleaned["daily_stock"]
        )

        return {
            "hourly_stock_and_weather": merged_hourly_stock_and_weather,
            "daily_stock_and_weather": merged_daily_stock_and_weather,
        }

    @task_group
    def load_data(cleaned: dict, merged: dict):
        @task
        def load_current_weather_data(file_path):
            return _load_data(input_file_path=file_path, table_name="current_weather")

        @task
        def load_hourly_stock_and_weather_data(file_path):
            return _load_data(
                input_file_path=file_path, table_name="hourly_stock_and_weather"
            )

        @task
        def load_daily_stock_and_weather_data(file_path):
            return _load_data(
                input_file_path=file_path, table_name="daily_stock_and_weather"
            )

        load_current_weather_data(cleaned["current_weather"])
        load_hourly_stock_and_weather_data(merged["hourly_stock_and_weather"])
        load_daily_stock_and_weather_data(merged["daily_stock_and_weather"])

    extracted = extract_data()
    cleaned = clean_data(extracted)
    enriched = enrich_data(cleaned)
    merged = merge_data(cleaned, enriched)
    load_data(cleaned, merged)

    clear_data_folders() >> list(extracted.values())


run_etl()
