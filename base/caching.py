#4
from __future__ import annotations
import os
import logging
import time
import sqlite3
from typing import Callable, Generic, Iterable, Any, Self, Sequence, cast, overload, override, TypeVar, ParamSpec, TypeVarTuple, TypedDict
from pathlib import Path

from base import dates
from base.serialization import TypedSerializer
from base.algos import binary_search, BinarySearchEdge
from base.files import escape_filename, unescape_filename
from base.serialization import Serializer, Serializable

logger = logging.getLogger(__name__)

class NotCachedError(Exception):
    def __init__(self):
        super().__init__()

class Persistor:
    """
    Key value storage provider.
    The key can be any string.
    """
    def persist(self, key: str, data: object):
        raise NotImplementedError()
    def read[T](self, key: str, assert_type: type[T]|None=None) -> T:
        raise NotImplementedError()
    def delete(self, key: str):
        raise NotImplementedError()
    def keys(self) -> Iterable[str]:
        raise NotImplementedError()
    def has(self, key: str) -> bool:
        raise NotImplementedError()
    def migrate(self, target: Persistor):
        for key in self.keys():
            target.persist(key, self.read(key))
    def try_read(self, key: str, assert_type: type[T]|None=None) -> T|None:
        try:
            return self.read(key, assert_type)
        except NotCachedError:
            return None

class NullPersistor(Persistor):
    @override
    def persist(self, key, data):
        return
    @override
    def read[T](self, key: str, assert_type: type[T]|None = None): raise NotCachedError()
    @override
    def delete(self, key): return
    @override
    def keys(self) -> Iterable[str]: return []
    @override
    def has(self, key: str) -> bool: return False

class FilePersistor(Persistor):
    def __init__(self, root: Path, serializer: Serializer = TypedSerializer()):
        self.root = root
        self.serializer = serializer
    def _get_path(self, key: str) -> Path:
        path = self.root
        for segment in key.split("/"): path/=escape_filename(segment)
        return path
    @override
    def persist(self, key: str, data: object):
        path = self._get_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.serializer.serialize(data))
    @override
    def read(self, key: str, assert_type: type[T]|None=None) -> Any:
        path = self._get_path(key)
        if not path.exists(): raise NotCachedError()
        return self.serializer.deserialize(path.read_text(), assert_type)
    @override
    def delete(self, key: str):
        path = self._get_path(key)
        path.unlink(missing_ok=True)
    @override
    def keys(self) -> Iterable[str]:
        if not self.root.exists(): return
        for folder, _, files in os.walk(self.root):
            folder = Path(folder)
            for file in files:
                relative = (folder/file).relative_to(self.root)
                yield str.join("/",(unescape_filename(it) for it in relative.parts))
    @override
    def has(self, key: str) -> bool: return self._get_path(key).exists()

class SqlitePersistor(Persistor):
    def __init__(self, db_path: Path, table: str, serializer: Serializer = TypedSerializer()):
        db_path.parent.mkdir(parents=True, exist_ok=True)
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
    def read[T](self, key: str, assert_type: type[T]|None = None) -> T:
        result = self.conn.execute(f"""
            select value from [{self.table}] where key = ?
        """, (key,)).fetchone()
        if result is None: raise NotCachedError()
        return self.serializer.deserialize(result[0], assert_type)
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
    @override
    def has(self, key: str) -> bool:
        return self.conn.execute(f"""
            select count(*) from [{self.table}] where key = ?        
        """, (key,)).fetchone()[0] > 0
    def __del__(self):
        self.conn.close()

class MemoryPersistor(Persistor):
    data: dict[str, Any]
    def __init__(self):
        self.data = {}
    @override
    def persist(self, key: str, data: Any):
        self.data[key] = data
    @override
    def read[T](self, key: str, assert_type: type[T]|None = None) -> T:
        if key not in self.data:
            raise NotCachedError()
        ret = self.data[key]
        if assert_type: assert isinstance(ret, assert_type)
        return ret
    @override
    def delete(self, key: str):
        if key in self.data: self.data.pop(key)
    @override
    def keys(self) -> Iterable[str]: return self.data.keys()
    @override
    def has(self, key: str) -> bool: return key in self.data

class Metadata(Serializable):
    def __init__(self, partials: dict[str, float] = {}):
        self.partials = partials

S = TypeVar('S')
T = TypeVar('T')
Args = TypeVarTuple('Args')
Params = ParamSpec('Params')

class CachedSeriesDescriptor(Generic[S, *Args, T]):
    def __init__(self,
        func: Callable[[S, float, float, *Args], Sequence[T]],
        get_timestamp: Callable[[T], float],
        get_key: Callable[[S, *Args], str],
        get_persistor: Callable[[S, *Args], Persistor],
        get_timestep: Callable[[S, *Args], float],
        get_delay: Callable[[S, *Args], float],
        should_refresh: Callable[[S, float, float, *Args], bool],    
    ):
        self.func = func
        self.get_timestamp = get_timestamp
        self.get_key = get_key
        self.get_persistor = get_persistor
        self.get_timestep = get_timestep
        self.get_delay = get_delay
        self.should_refresh = should_refresh
    
    def _get_id(self, unix_time: float, time_step: float) -> int:
        return int(unix_time//time_step) if unix_time%time_step else int(unix_time//time_step)-1
    def _slots(self, instance: S, unix_from: float, unix_to: float, *args: *Args) -> Iterable[tuple[str, float, float]]:
        timestep = self.get_timestep(instance, *args)
        base_key = self.get_key(instance, *args)
        start_id, end_id = int(unix_from//timestep), self._get_id(unix_to, timestep)
        for id in range(start_id, end_id + 1):
            yield f"{base_key}/{id}", id*timestep, (id+1)*timestep
    def _meta_key(self, instance: S, *args: *Args): return f"{self.get_key(instance, *args)}/{Metadata.__name__}"    
    
    def cached_method(self, instance: S, unix_from: float, unix_to: float, *args: *Args) -> list[T]:
        persistor = self.get_persistor(instance, *args)
        meta_key = self._meta_key(instance, *args)
        unix_now = dates.unix() - self.get_delay(instance, *args)
        result: list[T] = []
        def extend(data: Sequence[T]):
            if not data: return
            first = binary_search(data, unix_from, key=self.get_timestamp, edge=BinarySearchEdge.LOW)
            last = binary_search(data, unix_to, key=self.get_timestamp,  edge=BinarySearchEdge.LOW)
            result.extend(data[first+1:last+1])

        meta = persistor.try_read(meta_key, Metadata) or Metadata()
        for key, slot_from, slot_to in self._slots(instance, unix_from, unix_to, *args):
            if slot_from >= unix_now: continue
            elif key in meta.partials and unix_to > meta.partials[key] and (
                slot_to <= unix_now or self.should_refresh(instance, meta.partials[key], unix_now, *args)
            ):
                data: list[T] = persistor.read(key)
                data.extend(self.func(instance, meta.partials[key], min(slot_to, unix_now), *args))
                persistor.persist(key, data)
                extend(data)
                if slot_to <= unix_now: del meta.partials[key]
                else: meta.partials[key] = unix_now
            elif persistor.has(key): extend(persistor.read(key))
            else:
                data = list(self.func(instance, slot_from, min(slot_to, unix_now), *args))
                persistor.persist(key, data)
                extend(data)
                if slot_to > unix_now: meta.partials[key] = unix_now
        persistor.persist(meta_key, meta)
        return result
    
    def invalidate(self, instance: S, unix_from: float, unix_to: float, *args: *Args) -> None:
        persistor = self.get_persistor(instance, *args)
        meta_key = self._meta_key(instance, *args)
        meta = persistor.try_read(meta_key, Metadata) or Metadata()
        for key, slot_from, slot_to in self._slots(instance, unix_from, unix_to, *args):
            if unix_from <= slot_from: # invalidate entire slot
                if key in meta.partials: del meta.partials[key]
                persistor.delete(key)
            elif persistor.has(key):
                if key in meta.partials and meta.partials[key] <= unix_from: continue
                data: list[T] = persistor.read(key)
                data = data[:binary_search(data, unix_from, key=self.get_timestamp, edge=BinarySearchEdge.LOW)+1]
                meta.partials[key] = unix_from
                persistor.persist(key, data)
        persistor.persist(meta_key, meta)

    @overload
    def __get__(self, instance: None, owner: type[S]) -> Self: ...
    @overload
    def __get__(self, instance: S, owner: type[S]) -> Callable[[float, float, *Args]]: ...
    def __get__(self, instance: S|None, owner: type[S]) -> Callable[[float, float, *Args], list[T]]|Self:
        if instance is None: return self
        else: return lambda unix_from, unix_to, *args: self.cached_method(instance, unix_from, unix_to, *args)

def cached_series(
    *,
    timestamp_fn: Callable[[T], float],
    key_fn: Callable[[S, *Args], str],
    persistor_fn: Persistor | Callable[[S, *Args], Persistor],
    timestep_fn: float | Callable[[S, *Args], float],
    live_delay_fn: float | Callable[[S, *Args], float] | None = 0,
    should_refresh_fn: float | Callable[[S, float, float, *Args], bool] = 0,
) -> Callable[[Callable[[S, float, float, *Args], Sequence[T]]], CachedSeriesDescriptor[S, *Args, T]]:
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
    def decorate(func: Callable[[S, float, float, *Args], Sequence[T]]) -> CachedSeriesDescriptor[S, *Args, T]:
        return CachedSeriesDescriptor(
            func,
            timestamp_fn,
            key_fn,
            persistor_fn if callable(persistor_fn) else (lambda self, *args: cast(Persistor, persistor_fn)),
            timestep_fn if callable(timestep_fn) else (lambda self, *args: cast(float, timestep_fn)),
            live_delay_fn if callable(live_delay_fn) else (lambda self, *args: -1.0e10) if live_delay_fn is None else (lambda self, *args: cast(float, live_delay_fn)),
            should_refresh_fn if callable(should_refresh_fn) else lambda self, fetch, now, *args: now-fetch > cast(float, should_refresh_fn)
        )
    return decorate


class CachedScalarData(Serializable, Generic[T]):
    def __init__(self, data: T, unix_time: float):
        self.data = data
        self.unix_time = unix_time

class CachedScalarDescriptor(Generic[S, *Args, T]):
    def __init__(
        self,
        func: Callable[[S, *Args], T],
        get_key: Callable[[S, *Args], str],
        get_persistor: Callable[[S, *Args], Persistor],
        refresh_after: float|None
    ):
        self.func = func
        self.get_key = get_key
        self.get_persistor = get_persistor
        self.refresh_after = refresh_after
    
    def cached_method(self, instance: S, *args: *Args) -> T:
        key = self.get_key(instance, *args)
        persistor = self.get_persistor(instance, *args)
        if persistor.has(key):
            data = persistor.read(key, CachedScalarData)
            if self.refresh_after is None or data.unix_time + self.refresh_after > dates.unix():
                return data.data
        result = self.func(instance, *args)
        persistor.persist(key, CachedScalarData(result, dates.unix()))
        return result
    
    def invalidate(self, instance: S, *args: *Args):
        key = self.get_key(instance, *args)
        persistor = self.get_persistor(instance, *args)
        persistor.delete(key)
    
    @overload
    def __get__(self, instance: None, owner: type[S]) -> Self: ...
    @overload
    def __get__(self, instance: S, owner: type[S]) -> Callable[[*Args], T]: ...
    def __get__(self, instance: S|None, owner: type[S]) -> Callable[[*Args], T]|Self:
        if instance is None: return self
        else: return lambda *args: self.cached_method(instance, *args)

def cached_scalar(
    *,
    key_fn: Callable[[S, *Args], str],
    persistor_fn: Persistor|Callable[[S, *Args], Persistor],
    refresh_after: float|None = None
) -> Callable[[Callable[[S, *Args], T]], CachedScalarDescriptor[S, *Args, T]]:
    def decorate(func: Callable[[S, *Args], T]) -> CachedScalarDescriptor[S, *Args, T]:
        return CachedScalarDescriptor(
            func,
            key_fn,
            persistor_fn if callable(persistor_fn) else lambda self, *args: cast(Persistor, persistor_fn),
            refresh_after
        )
    return decorate
