#5
from __future__ import annotations
import logging
from typing import Callable, Generic, Self, Sequence, cast, overload, TypeVar, ParamSpec, TypeVarTuple

from base import dates
from base.algos import lower_whole, upper_whole
from base.key_series_storage import KeySeriesStorage
from base.key_value_storage import KeyValueStorage
from base.serialization import Serializable

logger = logging.getLogger(__name__)

S = TypeVar('S')
T = TypeVar('T')
Args = TypeVarTuple('Args')
Params = ParamSpec('Params')

class CachedSeriesDescriptor(Generic[S, *Args, T]):
    def __init__(self,
        func: Callable[[S, float, float, *Args], Sequence[T]],
        get_timestamp: Callable[[T], float],
        get_key: Callable[[S, *Args], str],
        get_storage: Callable[[S, *Args], KeySeriesStorage],
        get_chunk_size: Callable[[S, *Args], float|None],
        get_delay: Callable[[S, *Args], float],
        should_refresh: Callable[[S, float, float, *Args], bool],  
    ):
        self.func = func
        self.get_timestamp = get_timestamp
        self.get_key = get_key
        self.get_storage = get_storage
        self.get_chunk_size = get_chunk_size
        self.get_delay = get_delay
        self.should_refresh = should_refresh
    
    def cached_method(self, instance: S, unix_from: float, unix_to: float, *args: *Args) -> Sequence[T]:
        key = self.get_key(instance, *args)
        storage = self.get_storage(instance, *args)
        chunk_size = self.get_chunk_size(instance, *args)
        unix_now = dates.unix() - self.get_delay(instance, *args)

        unix_to = min(unix_to, unix_now)
        unix_from = min(unix_from, unix_to)
        if unix_to == unix_from: return []
        
        # Extended scope
        if chunk_size:
            extended_from = lower_whole(unix_from, chunk_size)
            extended_to = min(upper_whole(unix_to, chunk_size), unix_now)
        else:
            extended_from = unix_from
            extended_to = unix_to
        
        key = self.get_key(instance, *args)
        for span_from, span_to in storage.missing_spans(key, extended_from, extended_to):
            if span_to != unix_now or self.should_refresh(instance, span_from, span_to, *args):
                storage.set(key, span_from, span_to, self.func(instance, span_from, span_to, *args))
        return storage.get(key, unix_from, unix_to)

    @overload
    def __get__(self, instance: None, owner: type[S]) -> Self: ...
    @overload
    def __get__(self, instance: S, owner: type[S]) -> Callable[[float, float, *Args], Sequence[T]]: ...
    def __get__(self, instance: S|None, owner: type[S]) -> Callable[[float, float, *Args], Sequence[T]]|Self:
        if instance is None: return self
        else: return lambda unix_from, unix_to, *args: self.cached_method(instance, unix_from, unix_to, *args)

def cached_series(
    *,
    timestamp_fn: Callable[[T], float],
    key_fn: Callable[[S, *Args], str],
    storage_fn: KeySeriesStorage | Callable[[S, *Args], KeySeriesStorage],
    chunk_size_fn: float | Callable[[S, *Args], float|None],
    live_delay_fn: float | Callable[[S, *Args], float] | None = 0,
    should_refresh_fn: float | Callable[[S, float, float, *Args], bool] = 0,
) -> Callable[[Callable[[S, float, float, *Args], Sequence[T]]], CachedSeriesDescriptor[S, *Args, T]]:
    def decorate(func: Callable[[S, float, float, *Args], Sequence[T]]) -> CachedSeriesDescriptor[S, *Args, T]:
        return CachedSeriesDescriptor(
            func,
            timestamp_fn,
            key_fn,
            storage_fn if callable(storage_fn) else (lambda self, *args: cast(KeySeriesStorage, storage_fn)),
            chunk_size_fn if callable(chunk_size_fn) else (lambda self, *args: cast(float, chunk_size_fn)),
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
        get_storage: Callable[[S, *Args], KeyValueStorage],
        refresh: Callable[[S, *Args], float]
    ):
        self.func = func
        self.get_key = get_key
        self.get_storage = get_storage
        self.refresh = refresh
    
    def cached_method(self, instance: S, *args: *Args) -> T:
        key = self.get_key(instance, *args)
        storage = self.get_storage(instance, *args)
        if storage.has(key):
            data = storage.get(key, CachedScalarData)
            if data.unix_time + self.refresh(instance, *args) > dates.unix():
                return data.data
        result = self.func(instance, *args)
        storage.set(key, CachedScalarData(result, dates.unix()))
        return result
    
    @overload
    def __get__(self, instance: None, owner: type[S]) -> Self: ...
    @overload
    def __get__(self, instance: S, owner: type[S]) -> Callable[[*Args], T]: ...
    def __get__(self, instance: S|None, owner: type[S]) -> Callable[[*Args], T]|Self:
        if instance is None: return self
        else: return lambda *args: self.cached_method(instance, *args)

def cached_scalar(
    *,
    key_fn: Callable[[S, *Args], str] = lambda it: "",
    storage_fn: KeyValueStorage|Callable[[S, *Args], KeyValueStorage],
    refresh_fn: float|Callable[[S, *Args], float] = float('+inf')
) -> Callable[[Callable[[S, *Args], T]], CachedScalarDescriptor[S, *Args, T]]:
    def decorate(func: Callable[[S, *Args], T]) -> CachedScalarDescriptor[S, *Args, T]:
        return CachedScalarDescriptor(
            func,
            key_fn,
            storage_fn if callable(storage_fn) else lambda self, *args: cast(KeyValueStorage, storage_fn),
            refresh_fn if callable(refresh_fn) else (lambda self, *args: cast(float, refresh_fn))
        )
    return decorate
