from numbers import Number
import requests
import logging
import json
import re
import time
import config
from typing import Callable, Any, override
from base import text
from http import HTTPStatus
from enum import Flag, auto

logger = logging.getLogger(__name__)

def find_host(url: str) -> str | None:
    host = re.search(r"https?://([^/]*)", url)
    if host:
        return host.group(1)
    return None

class BadResponseException(Exception):
    def __init__(self, url: str, response: requests.Response):
        self.module = find_host(url)
        self.url = url
        self.response = response
    def __str__(self) -> str:
        return f"Can't fetch from {self.module}. Url: '{self.url}'. Code: {self.response.status_code}. Text: '{self.response.text}'."

class TooManyRequestsException(Exception):
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

class Scraper:
    def get(
        self,
        url: str,
        *, 
        cookies: dict = {}, 
        headers: dict = {}, 
        params: dict|None = None, 
        origin: str|None = None, 
        check_response: bool = True
    ) -> requests.Response: ...
    def post(
        self,
        url: str,
        body: dict|list|str|Number|bool|None,
        *,
        cookies: dict = {}, 
        headers: dict = {}, 
        params: dict|None = None, 
        origin: str|None = None, 
        check_response: bool = True
    ) -> requests.Response: ...

_CHROME_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': '*/*'
}
class BrowserImpersonator(Scraper):
    @override
    def get(self, url: str, *, cookies: dict = {}, headers: dict = {}, params: dict | None = None, origin: str | None = None, check_response: bool = True) -> requests.Response:
        cookie = ";".join([f"{key}={value}" for key,value in cookies.items()]) if cookies else None
        response = requests.get(url, headers = {**_CHROME_HEADERS, 'Cookie': cookie, **headers}, params=params)
        logger.info(f"GET {url} ? {params} -> {response.status_code}")
        if config.http.log_response: logger.info(response.text)
        elif config.http.log_response_short: logger.info(text.shorter(response.text))
        if check_response: assert_response(url, response)
        return response
    @override
    def post(self, url: str, body: dict|list|str|Number|bool|None, *, cookies: dict = {}, headers: dict = {}, params: dict | None = None, origin: str | None = None, check_response: bool = True) -> requests.Response:
        response = requests.post(url, json=body, headers={**_CHROME_HEADERS, **headers})
        logger.info(f"POST {url} -> {response.status_code}")
        if config.http.log_request: logger.info("->" + json.dumps(body, indent = 4))
        elif config.http.log_request_short: logger.info("->" + text.shorter(json.dumps(body, indent=4)))
        if config.http.log_response: logger.info("<-" + response.text)
        elif config.http.log_response_short: logger.info("<-" + text.shorter(response.text))
        if check_response: assert_response(url, response)
        return response
    
scraper = BrowserImpersonator()

class BackupBehavior(Flag):
    DEFAULT = 0
    RERAISE = auto()
    SLEEP = auto()

class BackupException(Exception):
    def __init__(self, backup_time: float):
        super().__init__()
        self.backup_time = backup_time

def backup_timeout[T: Callable](
    *,
    exc_type: type = TooManyRequestsException,
    default_behavior: BackupBehavior = BackupBehavior.RERAISE,
    base_timeout: float = 30.0,
    backoff_factor: float = 2.0
) -> Callable[[T], T]:
    last_break: float|None = None
    last_exception: Exception|None = None
    last_timeout: float|None = None
    def decorate(func: T) -> T:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            behavior: BackupBehavior = default_behavior
            nonlocal last_break
            nonlocal last_exception
            nonlocal last_timeout
            if last_break and last_exception and last_timeout:
                backup_time = last_break + last_timeout - time.time()
                if backup_time > 0:
                    if BackupBehavior.SLEEP in behavior:
                        time.sleep(backup_time)
                    elif BackupBehavior.RERAISE in behavior:
                        last_exception.__traceback__ = None
                        raise BackupException(backup_time) from last_exception
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
                    if BackupBehavior.RERAISE in behavior:
                        raise BackupException(timeout) from ex
                    else:
                        return None
                raise
        return wrapper # type: ignore
    return decorate