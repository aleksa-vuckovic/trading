import time
import requests
from http import HTTPStatus
import re
from pathlib import Path
import logging
from enum import Enum
import json
from typing import Callable, Any
from enum import Enum, Flag, auto
import torch

logger = logging.getLogger(__name__)
CACHE = Path(__file__).parent.parent / "data" / "cache"

class StatCollector:
    def __init__(self, name: str):
        self.name = name
        self.clear()
    
    def update(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int:
        result = self._calculate(expect, output)
        if isinstance(result, (int, float)): self.__update(result)
        else: self.__update(float(result.item()))
        return result

    def _calculate(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int:
        pass

    def __update(self, value: float | int):
        self.last = value
        self.count += 1
        self.total += value
        self.running = self.total / self.count
    
    def clear(self):
        self.last = 0
        self.count = 0
        self.total = 0
        self.running = 0

    def to_dict(self) -> dict:
        return {'running': self.running, 'last': self.last}
    
    def __str__(self):
        return f"{self.name}={self.running:.3f}({self.last:.2f})"

class StatContainer:
    stats: list[StatCollector]
    def __init__(self, *args, name: str | None = None):
        for arg in args:
            if not isinstance(arg, StatCollector):
                raise Exception(f'Unexpected arg type {type(arg)}')
        self.stats = list(args)
        self.name = name

    def update(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int | None:
        result = [it.update(expect, output) for it in self.stats]
        return result[0] if result else None
    
    def clear(self):
        [it.clear() for it in self.stats]
    
    def __str__(self):
        return ','.join([str(it) for it in self.stats])
    
    def to_dict(self):
        result = {it.name: it.to_dict() for it in self.stats}
        return {self.name: result} if self.name else result


def normalize_in_place(tensor: torch.Tensor, start_index: int = 0, count: int = -1, dim: int = 0) -> torch.Tensor:
    """
    Divides elements from start_index by the value of the largest element.
    For each batch separately.
    Returns the array of values used the normalize each batch, of shape (batches,)
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims:
        raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index
    j = start_index+count if count>=0 else tensor.shape[dim]
    if tensor.shape[dim] < j:
        raise Exception(f"End index {j} not valid form dimension {dim} of shape {tensor.shape}.")
    index = tuple()
    for it in range(total_dims):
        if dim == it:
            index += (slice(i,j),)
        else:
            index += (slice(None),)
    maxes, indices = torch.max(tensor[index], dim=dim, keepdim=True)
    tensor[index] = tensor[index] / maxes
    return maxes

class BinarySearchEdge(Enum):
    LOW ='low'
    HIGH = 'high'
    NONE = 'none'
def binary_search(
    collection: list, key: Callable[[Any], int|float], value: int|float, edge: BinarySearchEdge = BinarySearchEdge.NONE) -> int | None:
    """
    Returns the index of value.
    If the value is not there, returns the index of:
        - The last smaller value (LOW).
        - The first bigger value (HIGH).
        - None (NONE).
    The collection is assumed to be sorted in ascending order, based on the key.
    """
    if not collection:
        return None
    i = 0 # Always strictly smaller
    j = len(collection)-1 # Always strictly larger
    #Ensure proper initial conditions
    ival = key(collection[i])
    jval = key(collection[j])
    if ival == value:
        return i
    if jval == value:
        return j
    if ival > value:
        return i if edge == BinarySearchEdge.HIGH else None
    if jval < value:
        return j if edge == BinarySearchEdge.LOW else None
    while j - i > 1:
        mid = (i + j) // 2
        midval = key(collection[mid])
        if midval == value:
            return mid
        if midval > value:
            j = mid
        else:
            i = mid
    return j if edge == BinarySearchEdge.HIGH else i if edge == BinarySearchEdge.LOW else None

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
    def __str__(self):
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
    
    def __str__(self):
        return f"Too many requests for {self.module}. Url: {self.url}. Code: {self.response.status_code}. Text: {self.response.text}."

def check_response(url: str, response: requests.Response):
    if response.status_code == HTTPStatus.TOO_MANY_REQUESTS or response.status_code == HTTPStatus.FORBIDDEN:
        raise TooManyRequestsException(url, response)
    if response.status_code != 200:
        raise BadResponseException(url, response)
    
class BackupBehavior(Flag):
    DEFAULT = 0
    RETHROW = auto()
    SLEEP = auto()

def backup_timeout(
    *,
    exc_type = TooManyRequestsException,
    behavior: BackupBehavior = BackupBehavior.SLEEP | BackupBehavior.RETHROW,
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

def cached_series(
    cache_root: Path,
    *,
    unix_from_arg: str | int = 1,
    unix_to_arg: str | int = 2,
    include_args: list[str | int] | str | int = [],
    time_step_fn: int | float | Callable[[list], float] = lambda include_args: 10000000,
    series_field: str | None = None,
    timestamp_field: str | int = "unix_time",
    live_delay: float = 3600,
    return_series_only: bool = False
):
    """
    Denotes a method which returns time series data, based on unix timestamps, and whose results should be cached.
    The result MUST be a dictionary, list or None, and the time series part MUST be a list or None.
    The time series should be validated and filtered already.
    The underlying method is assumed to be closed at the start and open at the end of the interval.
        The return of this method is guaranteed to be closed at the start and open at the end.
    It is also assumed to return data sorted by timestamp!
    Args:
        cache_root - The root folder path where cache files are to be stored.
        include_args - Arguments to be included as time series inputs.
        time_step - The maximum time step with which to query the underlying method.
        time_step_fn - Determines the time step used when fetching the data. Can be a constant, or a method with one argument - the include_args list.
        series_field - The json path to locate the time series part of the object (can be empty if it's just the series itself).
        timestamp_field - The field within a single time series object containing the timestamp value. That field must always contain a valid value!
        live_delay - The maximum delay allowed for recent data.
        return_series_only - Wether to return just the series or the entire object.
    Returns:
        The time series list or the entire object as returned from the last underlying method call,
        with the proper series set.

    """
    cache_root.mkdir(parents=True, exist_ok=True)
    series_path = [int(it) if re.fullmatch(r"\d+", it) else it for it in (series_field or "").split(".") if it]
    include_args = include_args if isinstance(include_args, list) else [include_args]
    def get_series(data) -> list:
        try:
            ret = data
            for it in series_path:
                ret = data[it]
            return ret or []
        except:
            logger.error("Failed to extract time series.", exc_info=True)
            return []
    def set_series(data, series):
        if not series_path:
            return series
        try:
            for i in range(len(series_path)-1):
                data = data[series_path[i]]
            data[series_path[-1]] = series
        except:
            logger.error("Failed to set series.", exc_info=True)
        return data
    def get_timestamp(time_series_object) -> float:
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
            
            unix_now = time.time()
            start_id = int(unix_from) // time_step
            end_id = (int(unix_to) if unix_to%1 else int(unix_to)-1) // time_step
            now_id = int(unix_now) // time_step
            result = []
            last_data = None
            def extend(data):
                nonlocal last_data
                last_data = data
                series = get_series(data)
                first = binary_search(series, get_timestamp, unix_from, BinarySearchEdge.HIGH)
                last = binary_search(series, get_timestamp, unix_to, BinarySearchEdge.HIGH)
                if first is None or last==0:
                    return
                result.extend(series[first:last])

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
                        subpath.write_text(json.dumps(data))
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
            return  result if return_series_only else set_series(last_data, result)
        return wrapper
    return decorate