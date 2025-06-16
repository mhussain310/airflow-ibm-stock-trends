from datetime import datetime, timedelta


def load_date_config():
    current_date = datetime.today()
    previous_date_30_days = current_date - timedelta(days=30)

    previous_date_31_days = previous_date_30_days - timedelta(days=1)
    previous_date_60_days = previous_date_31_days - timedelta(days=29)

    previous_date_61_days = previous_date_60_days - timedelta(days=1)
    previous_date_90_days = previous_date_61_days - timedelta(days=29)

    current_date_str = current_date.strftime("%Y-%m-%d")
    previous_date_str = previous_date_30_days.strftime("%Y-%m-%d")

    previous_date_31_days_str = previous_date_31_days.strftime("%Y-%m-%d")
    previous_date_60_days_str = previous_date_60_days.strftime("%Y-%m-%d")

    previous_date_61_days_str = previous_date_61_days.strftime("%Y-%m-%d")
    previous_date_90_days_str = previous_date_90_days.strftime("%Y-%m-%d")

    return [
        (current_date_str, previous_date_str),
        (previous_date_31_days_str, previous_date_60_days_str),
        (previous_date_61_days_str, previous_date_90_days_str),
    ]
