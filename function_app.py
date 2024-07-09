import logging
import os
import azure.functions as func
import requests
from retrying import retry

TIMEOUT_REQUEST_SECS = 300
# Maximum number of retries
MAX_RETRIES = 5
# Exponential backoff settings
INITIAL_WAIT = 1000  # in milliseconds
MAX_WAIT = 10000  # in milliseconds

azure_storage_account_connect = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
data_source_key = os.environ.get("DATA_SOURCE_KEY")
token = "some_token"
url = "www.example.com"
app = func.FunctionApp()


@app.blob_trigger(arg_name="myblob", path=data_source_key,
                  connection=azure_storage_account_connect)
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob Name: {myblob.name}")
    upload_file(http_token=token, data=myblob.read(), http_url=url)


def is_retryable_exception(exception):
    """Return True if the exception is retryable."""
    return isinstance(exception, requests.exceptions.RequestException)


def should_retry(response):
    """Return True if the response status code indicates a retry should be attempted."""
    return response.status_code not in [403, 404, 429, 200]


@retry(
    wait_exponential_multiplier=INITIAL_WAIT,
    wait_exponential_max=MAX_WAIT,
    stop_max_attempt_number=MAX_RETRIES,
    retry_on_exception=is_retryable_exception,
    retry_on_result=should_retry
)
def upload_file(data, http_token, http_url):
    """Send a file via HTTP POST request with retries on failures."""
    human_readable = human_readable_size(len(data))
    print(f"Human-readable size: {human_readable}")
    headers = {'Authorization': f'Bearer {http_token}'}
    response = requests.post(url=http_url, data=data, headers=headers, timeout=TIMEOUT_REQUEST_SECS)
    if response.status_code == 200:
        logging.info("file upload successful!!")
        return response
    if response.status_code in [403, 404, 429]:
        logging.error(f'Failed to post data with http status code: '
                      f'{response.status_code} and response: {response.text}')
    response.raise_for_status()  # Raise an exception for HTTP error responses
    return response


def human_readable_size(byte_size, decimal_places=2):
    """
    Convert a byte size to a human-readable format.

    :param byte_size: The size in bytes.
    :param decimal_places: Number of decimal places for formatting.
    :return: Human-readable size string.
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
        if byte_size < 1024.0:
            return f"{byte_size:.{decimal_places}f} {unit}"
        byte_size /= 1024.0
    return f"{byte_size} B"

