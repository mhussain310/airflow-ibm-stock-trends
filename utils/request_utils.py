import requests


def get_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response

    except requests.exceptions.HTTPError as http_err:
        return f"HTTP error occurred: {http_err}"
    except requests.exceptions.ConnectionError as connection_err:
        return f"Connection error occurred: {connection_err}"
    except requests.exceptions.RequestException as err:
        raise f"Unexpected error occurred: {err}"
