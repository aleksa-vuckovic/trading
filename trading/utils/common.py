import time
import requests
from http import HTTPStatus
import re
from pathlib import Path
from logging import Logger
from enum import Enum
import json
from typing import Callable

CACHE = Path(__file__).parent.parent / "data" / "cache"

def _find_host(url: str) -> str | None:
    host = re.search(r"https?://(www.)?([^/\.]*)", url)
    if host:
        return host.group(2)
    return None

class BadResponseException(Exception):
    module: str
    url: str
    response: requests.Response
    def __init__(self, url: str, response: requests.Response):
        self.module = _find_host(url)
        self.url = url
        self.response = response
    def __repr__(self):
        return f"Can't fetch from {self.module}. Url: '{self.url}'. Code: {self.response.status_code}. Text: '{self.response.text}'."

class TooManyRequestsException(Exception):
    module: str
    url: str
    response: requests.Response
    def __init__(self, url: str, response: requests.Response):
        super().__init__()
        self.module = _find_host(url)
        self.url = url
        self.response = response
    
    def __repr__(self):
        return f"Too many requests for {self.module}. Url: {self.url}. Text: {self.response.text}."

def check_response(url: str, response: requests.Response):
    if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
        raise TooManyRequestsException(url, response)
    if response.status_code != 200:
        raise BadResponseException(url, response)

def backup_timeout(*,
    exc_type = TooManyRequestsException,
    rethrow: bool = False,
    base_timeout: float = 10.0,
    backoff_factor: float = 2.0
):
    last_break = None
    last_exception = None
    last_timeout = None
    def decorate(func):
        def wrapper(*args, **kwargs):
            nonlocal last_break
            nonlocal last_exception
            nonlocal last_timeout
            now = time.time()
            if last_break and (last_break + last_timeout > time.time()):
                if rethrow:
                    raise last_exception
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
                    if 'logger' in kwargs and kwargs['logger']:
                        kwargs['logger'].error(f"Timing out for {timeout} ({func.__name__})", exc_info = True)
                    if rethrow:
                        raise
                    else:
                        return None
                raise
        return wrapper
    return decorate

def cached_series(
    cache_root: Path,
    *,
    unix_from_arg: str | int = 1,
    unix_to_arg: str | int = 2,
    include_args: list[str | int] = [],
    time_step_fn: int | float | Callable[[list], float] = lambda include_args: 10000000,
    series_field: str = "",
    timestamp_field: str | int = "unix_time",
    live_delay: float = 3600
):
    """
    Denotes a method which returns time series data, based on unix timestamps, and whose results should be cached.
    The result MUST be a dictionary, list or None, and the time series part MUST be a list or None.
    The time series should be validated and filtered already.
    The underlying method is assumed to be closed at the start and open at the end of the interval.
    Args:
        cache_root - The root folder path where cache files are to be stored.
        include_args - Arguments to be included as time series inputs.
        time_step - The maximum time step with which to query the underlying method.
        time_step_fn - Determines the time step used when fetching the data. Can be a constant, or a method with one argument - the include_args list.
        series_field - The json path to locate the time series part of the object (can be empty if it's just the series itself).
        timestamp_field - The field within a single time series object containing the timestamp value.
        live_delay - The maximum delay allowed for recent data.
    Returns:
        The time series list.
        Notice that the return type might be different from the underlying method,
        if the underlying method returns the time series as part of a larger object.
    """
    cache_root.mkdir(parents=True, exist_ok=True)
    series_path = [int(it) if re.fullmatch(r"\d+", it) else it for it in (series_field or "").split(".") if it]
    def get_series(data, *, logger: Logger = None) -> list:
        try:
            ret = data
            for it in series_path:
                ret = data[it]
            return ret or []
        except:
            logger and logger.error("Failed to extract time series.", exc_info=True)
            return []
    def get_timestamp(time_series_object) -> float | None:
        return float(time_series_object[timestamp_field])
    def get_unix_args(args: list, kwargs: dict) -> tuple[float, float]:
        return float(args[unix_from_arg] if isinstance(unix_from_arg, int) else kwargs[unix_from_arg]), float(args[unix_to_arg] if isinstance(unix_to_arg, int) else kwargs[unix_to_arg])
    def set_unix_args(args: list, kwargs: dict, unix_from: float, unix_to: float):
        if isinstance(unix_from_arg, int):
            args[unix_from_arg] = unix_from
        else:
            kwargs[unix_from_arg] = unix_from
        if isinstance(unix_to_arg, int):
            args[unix_to_arg] = unix_to
        else:
            kwargs[unix_to_arg] = unix_to
    def decorate(func):
        def wrapper(*args, **kwargs):
            args = list(args)
            unix_from, unix_to = get_unix_args(args, kwargs)
            include_raw = [args[it] if isinstance(it, int) else kwargs[it] for it in include_args]
            include_str = [it.name if isinstance(it, Enum) else str(it) for it in include_raw]
            include = "-".join(include_str)
            path = cache_root / include if include else cache_root
            path.mkdir(parents=True, exist_ok=True)
            time_step = int(time_step_fn(include_raw) if callable(time_step_fn) else int(time_step_fn))
            logger: Logger | None = None
            if 'logger' in kwargs:
                logger = kwargs['logger']
            
            unix_now = time.time()
            start_id = int(unix_from) // time_step
            end_id = (int(unix_to) if unix_to%1 else int(unix_to)-1) // time_step
            now_id = int(unix_now) // time_step
            result = []
            def extend(data):
                series = get_series(data, logger=logger)
                i = 0
                j = len(series)
                while i < len(series) and get_timestamp(series[i]) < unix_from:
                    i += 1
                while (j > 0 and get_timestamp(series[i]) >= unix_to):
                    j -= 1
                result.extend(series[i:j])
                

            for id in range(start_id, min(end_id, now_id)+1):
                if id == now_id:
                    #live data
                    until = min(unix_now, float((id+1)*time_step), unix_to)
                    subpath = path / "live"
                    metapath = path / "meta"
                    #Check if meta exists and make sure the live id is current.
                    if not metapath.exists():
                        meta = {"live": None}
                    else:
                        meta = json.loads(metapath.read_text())
                    if meta["live"] and meta["live"]["id"] != id:
                        meta["live"] = None
                        subpath.unlink()
                    if not meta["live"]:
                        set_unix_args(args, kwargs, float(id*time_step), unix_now)
                        data = func(*args, **kwargs)
                        subpath.write_text(json.dumps(data))
                        extend(data)
                        meta["live"] = {"id": id, "fetch": unix_now}
                    elif meta["live"]["fetch"] < min(until, unix_now - live_delay):
                        data = json.loads(subpath.read_text())
                        set_unix_args(args, kwargs, meta["live"]["fetch"], unix_now)
                        new_data = func(*args, **kwargs)
                        get_series(data).extend(get_series(new_data))
                        extend(data)
                        subpath.write_text(json.dumps(new_data))
                        meta["live"] = {"id": id, "fetch": unix_now}
                    else:
                        data = json.loads(subpath.read_text())
                        extend(data)
                    metapath.write_text(json.dumps(meta))
                else:
                    #non live data
                    subpath = path / str(id)
                    if subpath.exists():
                        extend(json.loads(subpath.read_text()))
                    else:

                        # Invoke the underlying method for the entire chunk
                        set_unix_args(args, kwargs, float(id*time_step), float((id+1)*time_step))
                        data = func(*args, **kwargs)
                        subpath.write_text(json.dumps(data))
                        extend(data)
            return result
        return wrapper
    return decorate