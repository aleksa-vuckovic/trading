#2
from __future__ import annotations
import os
import logging
import time
import sqlite3
from typing import Callable, Iterable, Any, Sequence, override, TypeVar, ParamSpec, TypeVarTuple, TypedDict
from pathlib import Path
from base.serialization import TypedSerializer
from base.algos import binary_search, BinarySearchEdge
from base.files import escape_filename, unescape_filename
from base.serialization import Serializer

logger = logging.getLogger(__name__)

class NotCachedError(Exception):
    def __init__(self):
        super().__init__()

class Persistor:
    """
    Key value storage provider.
    """
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
    def try_read(self, key: str) -> Any:
        try:
            return self.read(key)
        except NotCachedError:
            return None

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
    def __init__(self, root: Path, serializer: Serializer = TypedSerializer()):
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
    def __init__(self, db_path: Path, table: str, serializer: Serializer = TypedSerializer()):
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
            insert or replace into [{self.table}](key, value) values (?, ?)
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

class MetaDict(TypedDict):
    live_fetch: float|None
    live_id: int|None

S = TypeVar('S')
T = TypeVar('T')
PKeys = TypeVarTuple('PKeys')

def cached_series(
    *,
    timestamp_fn: Callable[[T], float],
    key_fn: Callable[[S, *PKeys], str],
    persistor_fn: Persistor | Callable[[S, *PKeys], Persistor],
    time_step_fn: float | Callable[[S, *PKeys], float],
    live_delay_fn: float | Callable[[S, *PKeys], float] | None = 0,
    should_refresh_fn: float | Callable[[S, float, float, *PKeys], bool] = 0,
) -> Callable[[Callable[[S, float, float, *PKeys], Sequence[T]]], Callable[[S, float, float, *PKeys], list[T]]]:
    """
    Denotes a method which returns time series data, based on unix timestamps, and whose results should be cached.
    The return value of the method can be a dictionary or a list. The series_field denotes the path to the time series,
    which is the only relevant part. The time series should be validated, filtered and sorted, with no null timestamps.
    Intervals are considered OPEN at the start and CLOSED at the end.

    Args:
        timestamp_field: The field within a single time series object containing the timestamp value.
        time_step_fn: How big is one chunk of data?
        live_delay_fn: How long to wait before trying to fetch live data?
        live_refresh_fn: Should live data be refereshed?
    Returns:
        The time series list or the entire object as returned from the last underlying method call, with the proper series set.
    """
    get_timestamp = timestamp_fn
    get_key = key_fn
    get_persistor: Callable[[S, *PKeys], Persistor] = persistor_fn  if callable(persistor_fn) else (lambda self, *args: persistor_fn)
    get_time_step: Callable[[S, *PKeys], float] = time_step_fn if callable(time_step_fn) else (lambda self, *args: float(time_step_fn))
    get_delay: Callable[[S, *PKeys], float] = live_delay_fn if callable(live_delay_fn) else (lambda self, *args: -1.0e10) if live_delay_fn is None else (lambda self, *args: float(live_delay_fn))
    should_refresh: Callable[[S, float, float, *PKeys], bool] = should_refresh_fn if callable(should_refresh_fn) else lambda self, fetch, now, *args: now-fetch > float(should_refresh_fn)

    def get_id(unix_time: float, time_step: float) -> int:
        return int(unix_time//time_step) if unix_time%time_step else int(unix_time//time_step)-1
    def decorate(func: Callable[[S, float, float, *PKeys], Sequence[T]]) -> Callable[[S, float, float, *PKeys], list[T]]:
        def wrapper(self: S, unix_from: float, unix_to: float, *args: *PKeys) -> list[T]:
            base_key = get_key(self, *args)
            persistor = get_persistor(self, *args)
            time_step = get_time_step(self, *args)
            live_delay = get_delay(self, *args)
            meta_key = f"{base_key}/{MetaDict.__name__}"
            unix_now = time.time() - live_delay
            # Take at most the last available time point. We don't want to rush and cache invalid or nonexistent data.
            unix_to = min(unix_to, unix_now)
            unix_from = min(unix_from, unix_to)
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
                        meta: MetaDict|None = persistor.read(meta_key)
                    except NotCachedError:
                        meta = None
                    if not meta or not meta['live_id'] or not meta['live_fetch'] or meta['live_id'] != id:
                        data = func(self, float(id*time_step), unix_now, *args)
                        persistor.persist(key, list(data))
                        extend(data)
                        meta = {'live_id': id, 'live_fetch': unix_now}
                    elif meta['live_id'] < min(unix_to, unix_now) and should_refresh(self, meta['live_fetch'], unix_now, *args):
                        existing_data: list[T] = persistor.read(key)
                        new_data = func(self, meta['live_fetch'], unix_now, *args)
                        existing_data.extend(new_data)
                        extend(existing_data)
                        persistor.persist(key, existing_data)
                        meta = {'live_id': id, 'live_fetch': unix_now}
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
                        data = func(self, id*time_step, (id+1)*time_step, *args)
                        persistor.persist(key, data)
                        extend(data)
            return result
        return wrapper
    return decorate

P = ParamSpec('P')
def cached_scalar(
    *,
    key_fn: Callable[P, str],
    persistor_fn: Persistor|Callable[P,Persistor]
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    get_key: Callable[P, str] = key_fn
    get_persistor: Callable[P, Persistor] = persistor_fn if callable(persistor_fn) else lambda *args, **kwargs: persistor_fn
    def decorate(func: Callable[P, T]) -> Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            key = get_key(*args, **kwargs)
            persistor = get_persistor(*args, **kwargs)
            try:
                return persistor.read(key)
            except:
                result = func(*args, **kwargs)
                persistor.persist(key, result)
            return result
        return wrapper
    return decorate
