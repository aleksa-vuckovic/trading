#2
from __future__ import annotations
import json
import re
import os
import logging
import time
import sqlite3
from typing import Callable, Iterable, Any, Sequence, overload, override, TypeVar, ParamSpec
from pathlib import Path
from base.algos import binary_search, BinarySearchEdge, Comparable
from base.files import escape_filename, unescape_filename
from base.serialization import Serializer, BasicSerializer

logger = logging.getLogger(__name__)
CACHE_ROOT = Path(__file__).parent / 'cache'
DB_PATH = Path(__file__).parent.parent / 'cache.db'

LIVE = 'live'
FETCH = 'fetch'
NOW = 'now'

class NotCachedError(Exception):
    def __init__(self):
        super().__init__()

class Persistor:
    def persist(self, key: str, data: object):
        raise NotImplementedError()
    def read(self, key: str) -> Any:
        raise NotImplementedError()
    def delete(self, key: str):
        raise NotImplementedError()
    def keys(self) -> Iterable[str]:
        raise NotImplementedError()
    def migrate(self, target: Persistor):
        for key in self.keys():
            target.persist(key, self.read(key))

class NullPersistor(Persistor):
    @override
    def persist(self, key, data):
        return
    @override
    def read(self, key):
        raise NotCachedError()
    @override
    def delete(self, key):
        return
    @override
    def keys(self):
        return []

class FilePersistor(Persistor):
    def __init__(self, root: Path, serializer: Serializer = BasicSerializer()):
        self.root = root
        self.serializer = serializer
    def _get_path(self, key: str) -> Path:
        path = self.root
        if key:
            for segment in key.split("/"): path/=escape_filename(segment)
        return path
    @override
    def persist(self, key: str, data: object):
        path = self._get_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.serializer.serialize(data))
    @override
    def read(self, key: str) -> Any:
        path = self._get_path(key)
        if not path.exists(): raise NotCachedError()
        return self.serializer.deserialize(path.read_text())
    @override
    def delete(self, key):
        path = self._get_path(key)
        path.unlink(missing_ok=True)
    @override
    def keys(self):
        if not self.root.exists(): return
        if self.root.is_file(): yield ""
        for folder, _, files in os.walk(self.root):
            folder = Path(folder)
            for file in files:
                relative = (folder/file).relative_to(self.root)
                yield str.join("/",(unescape_filename(it) for it in relative.parts))

class SqlitePersistor(Persistor):
    def __init__(self, db_path: Path, table: str, serializer: Serializer = BasicSerializer()):
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.table = table
        self.serializer = serializer
        self.conn.execute(f"""
            create table if not exists [{self.table}] (
                key text primary key,
                value text
            )
        """)
    @override
    def persist(self, key: str, data: object):
        self.conn.execute(f"""
            insert into [{self.table}](key, value) values (?, ?)
        """, (key, self.serializer.serialize(data)))
    @override
    def read(self, key: str) -> Any:
        result = self.conn.execute(f"""
            select value from [{self.table}] where key = ?
        """, (key,)).fetchone()
        if result is None: raise NotCachedError()
        return self.serializer.deserialize(result[0])
    @override
    def delete(self, key: str):
        self.conn.execute(f"""
            delete from [{self.table}] where key = ?
        """, (key,))
    @override
    def keys(self) -> Iterable[str]:
        return (value for value, in self.conn.execute(f"""
            select key from [{self.table}]
        """))
    def __del__(self):
        if hasattr(self, 'conn'): self.conn.close()

class MemoryPersistor(Persistor):
    data: dict[str, Any]
    def __init__(self):
        self.data = {}
    @override
    def persist(self, key: str, data: Any):
        self.data[key] = data
    @override
    def read(self, key: str) -> Any:
        if key not in self.data:
            raise NotCachedError()
        return self.data[key]
    @override
    def delete(self, key: str):
        if key in self.data: self.data.pop(key)
    @override
    def keys(self) -> Iterable[str]:
        return self.data.keys()

P = ParamSpec('P')
T = TypeVar('T')
def cached_series(
    *,
    unix_args: tuple[str|int,str|int] = (1,2),
    timestamp_fn: Callable[[T], float],
    key_fn: Callable[..., str],
    persistor_fn: Persistor | Callable[..., Persistor],
    time_step_fn: float | Callable[..., float], #filtered args
    live_delay_fn: float | Callable[..., float] | None = 0, # filtered args
    should_refresh_fn: float | Callable[..., bool] = 0, # filtered args with 'last' and 'now' timestamps as kwargs
) -> Callable[[Callable[P, Sequence[T]]], Callable[P, list[T]]]:
    """
    Denotes a method which returns time series data, based on unix timestamps, and whose results should be cached.
    The return value of the method can be a dictionary or a list. The series_field denotes the path to the time series,
    which is the only relevant part. The time series should be validated, filtered and sorted, with no null timestamps.
    Intervals are considered OPEN at the start and CLOSED at the end.
    Set skip_cache kwarg to True to invoke underlying method with no additional behavior.

    Args:
        series_field: The json path to locate the time series part of the object.
        timestamp_field: The field within a single time series object containing the timestamp value.
        time_step_fn: How big is one chunk of data?
        live_delay_fn: How long to wait before trying to fetch live data?
        live_refresh_fn: Should live data be refereshed? With fetch and now as kwargs.
        return_series_only: If the series is nested, return the series or the entire object?
    Returns:
        The time series list or the entire object as returned from the last underlying method call, with the proper series set.
    """
    get_timestamp = timestamp_fn
    get_key = key_fn
    get_persistor: Callable[..., Persistor] = persistor_fn  if callable(persistor_fn) else lambda *args, **kwargs: persistor_fn
    get_time_step: Callable[..., float] = time_step_fn if callable(time_step_fn) else lambda *args, **kwargs: float(time_step_fn)
    get_delay: Callable[..., float] = live_delay_fn if callable(live_delay_fn) else (lambda *args, **kwargs: -1.0e10) if live_delay_fn is None else (lambda *args, **kwargs: float(live_delay_fn))
    should_refresh: Callable[..., bool] = should_refresh_fn if callable(should_refresh_fn) else lambda *args, **kwargs: kwargs[NOW]-kwargs[FETCH] > float(should_refresh_fn)

    def get_unix_args(args: list, kwargs: dict) -> tuple[float, float]:
        return tuple(float(args[it]) if isinstance(it, int) else float(kwargs[it]) for it in unix_args) # type: ignore
    def set_unix_args(args: list, kwargs: dict, values: tuple[float, float]):
        for arg, value in zip(unix_args, values):
            if isinstance(arg, int): args[arg] = value
            else: kwargs[arg] = value
    def get_id(unix_time: float, time_step: float) -> int:
        return int(unix_time//time_step) if unix_time%time_step else int(unix_time//time_step)-1
    def decorate(func: Callable[P, Sequence[T]]) -> Callable[P, list[T]]:
        def wrapper(*argstuple: Any, skip_cache: bool = False, **kwargs) -> list[T]:
            if skip_cache: return list(func(*argstuple, **kwargs))
            args = list(argstuple)
            unix_value = get_unix_args(args, kwargs)
            filtered_args = [it for i,it in enumerate(args) if i not in unix_args]
            filtered_kwargs = {key:kwargs[key] for key in kwargs if key not in unix_args}
            base_key = get_key(*filtered_args, **filtered_kwargs)
            persistor = get_persistor(*filtered_args, **filtered_kwargs)
            meta_key = f"{base_key}/meta"
            time_step = get_time_step(*filtered_args, **filtered_kwargs)
            live_delay = get_delay(*filtered_args, **filtered_kwargs)
            unix_now = time.time() - live_delay
            # Take at most the last available time point. We don't want to rush and cache invalid or nonexistent data.
            unix_to = min(unix_value[1], unix_now)
            unix_from = min(unix_value[0], unix_to)
            start_id, end_id, now_id = int(unix_from//time_step), get_id(unix_to, time_step), get_id(unix_now, time_step)
            result: list[T] = []
            def extend(data: Sequence[T]):
                if not data: return
                first = binary_search(data, unix_from, key=get_timestamp, edge=BinarySearchEdge.LOW)
                last = binary_search(data, unix_to, key=get_timestamp,  edge=BinarySearchEdge.LOW)
                result.extend(data[first+1:last+1])

            for id in range(start_id, end_id+1):
                if id == now_id:
                    #live data
                    key = f"{base_key}/live"
                    try:
                        meta: dict = persistor.read(meta_key)
                    except NotCachedError:
                        meta = {LIVE: None}
                    if meta[LIVE] and meta[LIVE]['id'] != id:
                        meta[LIVE] = None
                        logger.info(f"Switching to new id {id} from {meta[LIVE]}.")
                    if not meta[LIVE]:
                        set_unix_args(args, kwargs, (float(id*time_step), unix_now))
                        data = func(*args, **kwargs) # type: ignore
                        persistor.persist(key, list(data))
                        extend(data)
                        meta[LIVE] = {'id': id, FETCH: unix_now}
                    elif meta[LIVE][FETCH] < min(unix_to, unix_now) and should_refresh(*filtered_args, **filtered_kwargs, **{FETCH: meta[LIVE][FETCH], NOW: unix_now}):
                        existing_data: list[T] = persistor.read(key)
                        set_unix_args(args, kwargs, (meta[LIVE][FETCH], unix_now))
                        new_data = func(*args, **kwargs) # type: ignore
                        existing_data.extend(new_data)
                        extend(existing_data)
                        persistor.persist(key, existing_data)
                        meta[LIVE] = {'id': id, FETCH: unix_now}
                    else:
                        extend(persistor.read(key))
                    persistor.persist(meta_key, meta)
                else:
                    #non live data
                    key = f"{base_key}/{str(id)}"
                    try:
                        extend(persistor.read(key))
                    except NotCachedError:
                        # Invoke the underlying method for the entire chunk
                        set_unix_args(args, kwargs, (id*time_step, (id+1)*time_step))
                        data = func(*args, **kwargs) # type: ignore
                        persistor.persist(key, data)
                        extend(data)
            return result
        return wrapper # type: ignore
    return decorate

def cached_scalar[T: Callable](
    *,
    key_fn: Callable[..., str],
    persistor_fn: Persistor|Callable[...,Persistor]
) -> Callable[[T], T]:
    get_key = key_fn
    get_persistor = persistor_fn if callable(persistor_fn) else lambda *args, **kwargs: persistor_fn
    def decorate(func: T) -> T:
        def wrapper(*args, skip_cache: bool = False, **kwargs):
            if skip_cache: return func(*args, **kwargs)
            key = get_key(*args, **kwargs)
            persistor = get_persistor(*args, **kwargs)
            try:
                return persistor.read(key)
            except:
                result = func(*args, **kwargs)
                persistor.persist(key, result)
            return result
        return wrapper # type: ignore
    return decorate
