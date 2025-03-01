import requests
import logging
import json
import re
import time
import config
from http import HTTPStatus
from enum import Flag, auto
from ..utils import common

logger = logging.getLogger(__name__)

def find_host(url: str) -> str | None:
    host = re.search(r"https?://([^/]*)", url)
    if host:
        return host.group(1)
    return None

class BadResponseException(Exception):
    module: str
    url: str
    response: requests.Response
    def __init__(self, url: str, response: requests.Response):
        self.module = find_host(url)
        self.url = url
        self.response = response
    def __str__(self):
        return f"Can't fetch from {self.module}. Url: '{self.url}'. Code: {self.response.status_code}. Text: '{self.response.text}'."

class TooManyRequestsException(Exception):
    module: str
    url: str
    response: requests.Response
    def __init__(self, url: str|None=None, response: requests.Response|None=None):
        super().__init__()
        self.module = find_host(url) if url else None
        self.url = url
        self.response = response
    
    def __str__(self):
        return f"Too many requests for {self.module or '<unknown>'}. Url: {self.url or '<unknown>'}. Code: {self.response.status_code if self.response else '<unknown>'}. Text: {self.response.text if self.response else '<unknown>'}."

def assert_response(url: str, response: requests.Response):
    if response.status_code == HTTPStatus.TOO_MANY_REQUESTS or response.status_code == HTTPStatus.FORBIDDEN:
        raise TooManyRequestsException(url, response)
    if response.status_code != 200:
        raise BadResponseException(url, response)

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
    if config.http.log_response: logger.info(response.text)
    elif config.http.log_response_short: logger.info(common.shorter(response.text))
    if check_reponse: assert_response(url, response)
    return response

def post_as_browser(url: str, body: object, check_response: bool = True) -> requests.Response:
    response = requests.post(url, json=body, headers={**_CHROME_HEADERS})
    logger.info(f"POST {url} -> {response.status_code}")
    if config.http.log_request: logger.info("->" + json.dumps(body, indent = 4))
    elif config.http.log_request_short: logger.info("->" + common.shorter(json.dumps(body, indent=4)))
    if config.http.log_response: logger.info("<-" + response.text)
    elif config.http.log_response_short: logger.info("<-" + common.shorter(response.text))
    if check_response: assert_response(url, response)
    return response

class BackupBehavior(Flag):
    DEFAULT = 0
    RETHROW = auto()
    SLEEP = auto()

def backup_timeout(
    *,
    exc_type = TooManyRequestsException,
    default_behavior: BackupBehavior = BackupBehavior.RETHROW,
    base_timeout: float = 30.0,
    backoff_factor: float = 2.0
):
    last_break = None
    last_exception: Exception = None
    last_timeout = None
    def decorate(func):
        def wrapper(*args, **kwargs):
            nonlocal last_break
            nonlocal last_exception
            nonlocal last_timeout
            time_left: float = last_break and (last_break + last_timeout - time.time())
            if 'backup_behavior' in kwargs:
                behavior = kwargs['backup_behavior']
                del kwargs['backup_behavior']
            else: behavior = default_behavior
            if time_left and time_left > 0:
                if BackupBehavior.SLEEP in behavior:
                    time.sleep(time_left)
                if BackupBehavior.RETHROW in behavior:
                    last_exception.__traceback__ = None
                    raise last_exception from None
                else:
                    return None
            last_break = None
            last_exception = None
            timeout = last_timeout*backoff_factor if last_timeout else base_timeout
            last_timeout = None
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                if isinstance(ex, exc_type):
                    last_break = time.time()
                    last_exception = ex
                    last_timeout = timeout
                    logger.error(f"Timing {func.__name__} out for {timeout} with behavior {behavior}.", exc_info = True)
                    if BackupBehavior.RETHROW in behavior:
                        raise
                    else:
                        return None
                raise
        return wrapper
    return decorate