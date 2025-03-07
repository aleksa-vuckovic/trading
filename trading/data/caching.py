from __future__ import annotations
import json
import re
import os
import logging
import time
import sqlite3
from typing import Callable, Iterator
from pathlib import Path
from ..utils.common import escape_filename, unescape_filename, binary_search, BinarySearchEdge

logger = logging.getLogger(__name__)
CACHE_ROOT = Path(__file__).parent / 'cache'

FETCH = 'fetch'
NOW = 'now'

class NotCachedError(Exception):
    def __init__(self):
        super().__init__(self)

class Persistor:
    def persist(self, key: list[str], data: str):
        raise NotImplementedError()
    def read(self, key: list[str]) -> str:
        raise NotImplementedError()
    def delete(self, key: list[str]):
        raise NotImplementedError()
    def keys(self) -> Iterator[list[str]]:
        raise NotImplementedError()
    def migrate(self, target: Persistor):
        for key in self.keys():
            target.persist(key, self.read(key))

class FilePersistor(Persistor):
    def __init__(self, root: Path):
        self.root = root
    def _get_path(self, key: list[str]) -> Path:
        path = self.root
        for segment in key: path/=escape_filename(segment)
        return path
    def persist(self, key, data):
        path = self._get_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data)
    def read(self, key) -> str:
        path = self._get_path(key)
        if not path.exists(): raise NotCachedError()
        return path.read_text()
    def delete(self, key):
        path = self._get_path(key)
        path.unlink(missing_ok=True)
    def keys(self):
        if not self.root.exists(): return
        if self.root.is_file(): yield []
        for folder, _, files in os.walk(self.root):
            folder = Path(folder)
            for file in files:
                relative = (folder/file).relative_to(self.root)
                yield list(unescape_filename(it) for it in relative.parts)

class SqlitePersistor(Persistor):
    def __init__(self, db_path: Path, table: str, sep:str="|"):
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.table = table
        self.sep = sep
        self.conn.execute(f"""
            create table if not exists [{self.table}] (
                key text primary key,
                value text
            )
        """)
    def persist(self, key, data):
        self.conn.execute(f"""
            insert into [{self.table}](key, value) values (?, ?)
        """, (self.sep.join(key), data))
    def read(self, key):
        result = self.conn.execute(f"""
            select value from [{self.table}] where key = ?
        """, (self.sep.join(key),)).fetchone()
        if result is None: raise NotCachedError()
        return result[0]
    def delete(self, key):
        self.conn.execute(f"""
            delete from [{self.table}] where key = ?
        """, (self.sep.join(key),))
    def keys(self):
        return [value.split(self.sep) if self.sep in value else [] for value, in self.conn.execute(f"""
            select key from [{self.table}]
        """)]
    def __del__(self):
        if hasattr(self, 'conn'): self.conn.close()

def cached_series(
    *,
    unix_args: tuple[str|int,str|int] = (1,2),
    series_field: str | None = None,
    timestamp_field: str | int = "unix_time",
    key_fn: Callable[..., list[str]],
    persistor_fn: Persistor | Callable[..., Persistor],
    time_step_fn: float | int | Callable[..., float], #filtered args
    live_delay_fn: float | int | Callable[..., float|int] = 0, # filtered args
    should_refresh_fn: float | int | Callable[..., bool] = 0, # filtered args with 'last' and 'now' timestamps as kwargs
    return_series_only: bool = False
):
    """
    Denotes a method which returns time series data, based on unix timestamps, and whose results should be cached.
    The result MUST be a dictionary, list or None, and the time series part MUST be a list or None.
    The time series should be validated and filtered already.
    The underlying method is assumed to be OPEN at the start and CLOSED at the end of the interval.
        The return of this method is guaranteed to be OPEN at the start and CLOSED at the end.
    It is also assumed to return data sorted by timestamp!
    Args ending with fn should accept all args and kwargs, with the unix arguments filtered out.
    If the kwargs contain skip_cache set to True, the underlying method will be invoked directly.
    Args:
        series_field: The json path to locate the time series part of the object (can be empty if it's just the series itself).
        timestamp_field: The field within a single time series object containing the timestamp value. That field must always contain a valid value!
        time_step_fn: How big is one chunk of data?
        live_delay_fn: How long to wait before trying to fetch live data?
        live_refresh_fn: Should live data be refereshed, based on the last fetch timestamp and now timestamp.
        return_series_only: If the series is nested, return the series or the entire object?
    Returns:
        The time series list or the entire object as returned from the last underlying method call, with the proper series set.
    """
    series_path = [int(it) if re.fullmatch(r"\d+", it) else it for it in (series_field or "").split(".") if it]
    get_key = key_fn
    get_persistor = persistor_fn  if callable(persistor_fn) else lambda *args, **kwargs: persistor_fn
    get_time_step = time_step_fn if callable(time_step_fn) else lambda *args, **kwargs: float(time_step_fn)
    get_delay = live_delay_fn if callable(live_delay_fn) else lambda *args, **kwargs: float(live_delay_fn)
    should_refresh = should_refresh_fn if callable(should_refresh_fn) else lambda *args, **kwargs: kwargs[NOW]-kwargs[FETCH] > float(should_refresh_fn)

    def get_series(data) -> list:
        try:
            ret = data
            for it in series_path: ret = data[it]
            return ret if ret is not None else []
        except:
            logger.error("Failed to extract time series.", exc_info=True)
            return []
    def set_series(data, series):
        if not series_path:
            return series
        try:
            for i in range(len(series_path)-1): data = data[series_path[i]]
            data[series_path[-1]] = series
        except:
            logger.error("Failed to set series.", exc_info=True)
        return data
    def get_timestamp(time_series_object) -> float:
        return float(time_series_object[timestamp_field])
    def get_unix_args(args: list, kwargs: dict) -> tuple[float, float]:
        return tuple(float(args[it]) if isinstance(it, int) else float(kwargs[it]) for it in unix_args)
    def set_unix_args(args: list, kwargs: dict, value: tuple[float, float]):
        for arg, value in zip(unix_args, value):
            if isinstance(arg, int): args[arg] = value
            else: kwargs[arg] = value
    def decorate(func):
        def wrapper(*args, **kwargs):
            if 'skip_cache' in kwargs:
                skip_cache = kwargs['skip_cache']
                del kwargs['skip_cache']
                if skip_cache: return func(*args, **kwargs)
            args = list(args)
            unix_value = get_unix_args(args, kwargs)
            filtered_args = [it for i,it in enumerate(args) if i not in unix_args]
            filtered_kwargs = {key:kwargs[key] for key in kwargs if key not in unix_args}
            base_key = get_key(*filtered_args, **filtered_kwargs)
            persistor = get_persistor(*filtered_args, **filtered_kwargs)
            meta_key = base_key + ['meta']
            time_step = get_time_step(*filtered_args, **filtered_kwargs)
            live_delay = get_delay(*filtered_args, **filtered_kwargs)
            unix_now = time.time() - live_delay
            # Take at most the last available time point. We don't want to rush and cache invalid or nonexistent data.
            unix_to = min(unix_value[1], unix_now)
            unix_from = min(unix_value[0], unix_to)
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
                first = binary_search(series, unix_from, key=get_timestamp, edge=BinarySearchEdge.LOW)
                last = binary_search(series, unix_to, key=get_timestamp,  edge=BinarySearchEdge.LOW)
                result.extend(series[first+1:last+1])

            for id in range(start_id, end_id+1):
                if id == now_id:
                    #live data
                    key = base_key + ['live']
                    try:
                        meta = json.loads(persistor.read(meta_key))
                    except NotCachedError:
                        meta = {'live': None}
                    if meta['live'] and meta['live']['id'] != id:
                        meta['live'] = None
                        logger.info(f"Switching to new id {id} from {meta['live']}.")
                    if not meta['live']:
                        set_unix_args(args, kwargs, (float(id*time_step), unix_now))
                        data = func(*args, **kwargs)
                        persistor.persist(key, json.dumps(data))
                        extend(data)
                        meta['live'] = {'id': id, FETCH: unix_now}
                    elif meta['live'][FETCH] < min(unix_to, unix_now) and should_refresh(*filtered_args, **{**filtered_kwargs, FETCH: meta['live'][FETCH], NOW: unix_now}):
                        data = json.loads(persistor.read(key))
                        set_unix_args(args, kwargs, (meta['live'][FETCH], unix_now))
                        new_data = func(*args, **kwargs)
                        get_series(data).extend(get_series(new_data))
                        extend(data)
                        persistor.persist(key, json.dumps(data))
                        meta['live'] = {'id': id, FETCH: unix_now}
                    else:
                        extend(json.loads(persistor.read(key)))
                    persistor.persist(meta_key, json.dumps(meta))
                else:
                    #non live data
                    key = base_key + [str(id)]
                    try:
                        extend(json.loads(persistor.read(key)))
                    except NotCachedError:
                        # Invoke the underlying method for the entire chunk
                        set_unix_args(args, kwargs, (float(id*time_step)-1e-5, float((id+1)*time_step)-1e-5))
                        data = func(*args, **kwargs)
                        persistor.persist(key, json.dumps(data))
                        extend(data)
            return  result if return_series_only else set_series(last_data, result)
        return wrapper
    return decorate


def cached_scalar(
    *,
    key_fn: Callable[..., list[str]],
    persistor_fn: Persistor|Callable[...,Persistor]
):
    get_key = key_fn
    get_persistor = persistor_fn if callable(persistor_fn) else lambda *args, **kwargs: persistor_fn
    def decorate(func):
        def wrapper(*args, **kwargs):
            if 'skip_cache' in kwargs:
                skip_cache = kwargs['skip_cache']
                del kwargs['skip_cache']
                if skip_cache: return func(*args, **kwargs)
            key = get_key(*args, **kwargs)
            persistor = get_persistor(*args, **kwargs)
            try:
                return json.loads(persistor.read(key))
            except:
                result = func(*args, **kwargs)
                persistor.persist(key, json.dumps(result))
            return result
        return wrapper
    return decorate
