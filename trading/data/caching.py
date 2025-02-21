import json
import re
import logging
import time
from typing import Callable
from pathlib import Path
from enum import Enum
from ..utils.common import escape_filename, binary_search, BinarySearchEdge

logger = logging.getLogger(__name__)
CACHE_ROOT = Path(__file__).parent / 'cache'

def cached_series(
    *,
    unix_from_arg: str | int = 1,
    unix_to_arg: str | int = 2,
    include_args: list[str | int] | str | int = [],
    cache_root: Path | None = None,
    path_fn: Callable[[list], Path] = None,
    time_step_fn: int | float | Callable[[list], float] = lambda include_args: 10000000.0,
    series_field: str | None = None,
    timestamp_field: str | int = "unix_time",
    live_delay_fn: float | int | Callable[[list], float|int] = 3600, #Difference between data at time t, and time t' when the data is actually available.
    refresh_delay_fn: float | int | Callable[[list], float|int] | None = None, #Minimum time between 2 live fetches. Default - equal to live delay.
    return_series_only: bool = False
):
    """
    Denotes a method which returns time series data, based on unix timestamps, and whose results should be cached.
    The result MUST be a dictionary, list or None, and the time series part MUST be a list or None.
    The time series should be validated and filtered already.
    The underlying method is assumed to be OPEN at the start and CLOSED at the end of the interval.
        The return of this method is guaranteed to be OPEN at the start and CLOSED at the end.
    It is also assumed to return data sorted by timestamp!
    Args:
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
    if cache_root is None and path_fn is None:
        raise Exception('At least one of cache_root or path_fn must be set.')
    series_path = [int(it) if re.fullmatch(r"\d+", it) else it for it in (series_field or "").split(".") if it]
    include_args = include_args if isinstance(include_args, list) else [include_args]
    if refresh_delay_fn is None: refresh_delay_fn = live_delay_fn
    def get_series(data) -> list:
        try:
            ret = data
            for it in series_path:
                ret = data[it]
            return ret if ret is not None else []
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
            if 'skip_cache' in kwargs:
                skip_cache = kwargs['skip_cache']
                del kwargs['skip_cache']
                if skip_cache: return func(*args, **kwargs)
            args = list(args)
            unix_from, unix_to = get_unix_args(args, kwargs)
            include = [args[it] if isinstance(it, int) else kwargs[it] for it in include_args]
            live_delay = live_delay_fn(include) if callable(live_delay_fn) else live_delay_fn
            refresh_delay = refresh_delay_fn(include) if callable(refresh_delay_fn) else refresh_delay_fn
            if cache_root:
                path = cache_root
                if include:
                    for arg in include:
                        path /= escape_filename(arg.name if isinstance(arg, Enum) else str(arg))
            else:
                path = path_fn(include)
            path.mkdir(parents=True,exist_ok=True)
            time_step = int(time_step_fn(include) if callable(time_step_fn) else int(time_step_fn))
            
            unix_now = time.time() - live_delay
            # Take at most the last available time point. We don't want to rush and cache invalid or nonexistent data.
            unix_to = min(unix_to, unix_now)
            unix_from = min(unix_from, unix_to)
            start_id = int(unix_from//time_step)
            end_id = int(unix_to//time_step)
            now_id = int(unix_now//time_step)
            result = []
            last_data = None
            def extend(data):
                nonlocal last_data
                last_data = data
                series = get_series(data)
                if not series: return
                first = binary_search(series, get_timestamp, unix_from, BinarySearchEdge.LOW)
                last = binary_search(series, get_timestamp, unix_to, BinarySearchEdge.LOW)
                if last is None: return
                if first is None: result.extend(series[:last+1])
                else: result.extend(series[first+1:last+1])

            for id in range(start_id, end_id+1):
                if id == now_id:
                    #live data
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
                    elif meta["live"]["fetch"] < min(unix_to, unix_now - refresh_delay):
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
                        set_unix_args(args, kwargs, float(id*time_step)-1e-5, float((id+1)*time_step)-1e-5)
                        data = func(*args, **kwargs)
                        subpath.write_text(json.dumps(data))
                        extend(data)
            return  result if return_series_only else set_series(last_data, result)
        return wrapper
    return decorate


def cached_scalar(
    *,
    include_args: list[str | int] | str | int = [],
    cache_root: Path | None = None,
    path_fn: Callable[[list], Path] = None
):
    """
    Caches the function return values, per unique set of include_args.
    Either cache_root OR path has to be set.
        If cache_root is set, the cached response is stored in cache_root/all/include/args.
        If path is set, it's invoked with the raw include_args and a Path is expected.
    """
    if cache_root is None and path_fn is None:
        raise Exception('At least one of cache_root or path_fn must be set.')
    include_args = include_args if isinstance(include_args, list) else [include_args]
    def decorate(func):
        def wrapper(*args, **kwargs):
            include = [args[it] if isinstance(it, int) else kwargs[it] for it in include_args]
            if cache_root:
                path = cache_root
                if include:
                    for arg in include:
                        path /= escape_filename(arg.name if isinstance(arg, Enum) else str(arg))
            else:
                path = path_fn(include)
            if path.exists():
                return json.loads(path.read_text())
            path.parent.mkdir(parents=True,exist_ok=True)
            result = func(*args, **kwargs)
            path.write_text(json.dumps(result))
            return result
        return wrapper
    return decorate
