import requests
import logging
import json
from . import common

logger = logging.getLogger(__name__)

_CHROME_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': '*/*'
}
def get_as_browser(
    url: str,
    *,
    cookies: dict = {},
    params: dict = None,
    origin: str = None,
    headers: dict = {},
    check_reponse: bool = True
) -> requests.Response:
    cookie = ";".join([f"{key}={value}" for key,value in cookies.items()]) if cookies else None
    response = requests.get(url, headers = {**_CHROME_HEADERS, 'Cookie': cookie, **headers}, params=params)
    logger.info(f"GET {url} ? {params} -> {response.status_code}")
    if check_reponse:
        common.check_response(url, response)
    return response

def post_as_browser(url: str, body: object, check_response: bool = True) -> requests.Response:
    response = requests.post(url, json=body, headers={**_CHROME_HEADERS})
    logger.info(f"POST {url} -> {response.status_code}")
    logger.info(json.dumps(body, indent = 4))
    if check_response:
        common.check_response(url, response)
    return response