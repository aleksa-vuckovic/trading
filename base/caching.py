#5
from __future__ import annotations
import logging
from typing import Callable, Generic, Iterable, Self, Sequence, cast, overload, TypeVar, ParamSpec, TypeVarTuple
from base import dates
from base.algos import binary_search, lower_whole, upper_whole
from base.key_series_storage import KeySeriesStorage
from base.key_value_storage import KeyValueStorage, NotFoundError

logger = logging.getLogger(__name__)

S = TypeVar('S')
T = TypeVar('T')
Args = TypeVarTuple('Args')

_LAST_FETCH='__last_fetch'
class CachedScalarDescriptor(Generic[S, *Args, T]):
    def __init__(
        self,
        func: Callable[[S, *Args], T],
        get_key: Callable[[S, *Args], str],
        get_storage: Callable[[S, *Args], KeyValueStorage],
        get_refresh_interval: Callable[[S, *Args], float|None]
    ):
        self.func = func
        self.get_key = get_key
        self.get_storage = get_storage
        self.get_refresh_interval = get_refresh_interval
    
    def cached_method(self, instance: S, *args: *Args) -> T:
        key = self.get_key(instance, *args)
        assert not key.endswith(_LAST_FETCH)
        storage = self.get_storage(instance, *args)
        refresh_interval = self.get_refresh_interval(instance, *args)
        try:
            data = storage.get(key)
            unix_time = storage.get(f"{key}{_LAST_FETCH}")
            if unix_time + refresh_interval > dates.unix(): return data
        except NotFoundError:
            pass
        result = self.func(instance, *args)
        storage.set(key, result)
        storage.set(f"{key}{_LAST_FETCH}", dates.unix())
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
    key: Callable[[S, *Args], str] = lambda self, *args: "_".join(str(it) for it in args),
    storage: KeyValueStorage|Callable[[S, *Args], KeyValueStorage],
    refresh_interval: float|Callable[[S, *Args], float] = float('+inf')
) -> Callable[[Callable[[S, *Args], T]], CachedScalarDescriptor[S, *Args, T]]:
    def decorate(func: Callable[[S, *Args], T]) -> CachedScalarDescriptor[S, *Args, T]:
        return CachedScalarDescriptor(
            func,
            key,
            storage if callable(storage) else lambda self, *args: cast(KeyValueStorage, storage),
            refresh_interval if callable(refresh_interval) else (lambda self, *args: cast(float, refresh_interval))
        )
    return decorate

Params = ParamSpec('Params')

class CachedSeriesDescriptor(Generic[S, *Args, T]):
    """This implementation assumes that the underlying storage will never be deleted from."""
    def __init__(
        self,
        func: Callable[[S, float, float, *Args], Sequence[T]],
        get_key: Callable[[S, *Args], str],
        get_kv_storage: Callable[[S, *Args], KeyValueStorage],
        get_ks_storage: Callable[[S, *Args], KeySeriesStorage[T]],
        get_min_chunk: Callable[[S, *Args], float|None],
        get_max_chunk: Callable[[S, *Args], float|None],
        get_delay: Callable[[S, *Args], float],
        should_refresh: Callable[[S, float, float, *Args], bool],  
    ):
        self.func = func
        self.get_key = get_key
        self.get_kv_storage = get_kv_storage
        self.get_ks_storage = get_ks_storage
        self.get_min_chunk = get_min_chunk
        self.get_max_chunk = get_max_chunk
        self.get_delay = get_delay
        self.should_refresh = should_refresh
    
    def cached_method(self, instance: S, unix_from: float, unix_to: float, *args: *Args) -> Sequence[T]:
        key = self.get_key(instance, *args)
        kv_storage = self.get_kv_storage(instance, *args)
        ks_storage = self.get_ks_storage(instance, *args)
        min_chunk = self.get_min_chunk(instance, *args)
        max_chunk = self.get_max_chunk(instance, *args)
        unix_now = dates.unix() - self.get_delay(instance, *args)

        unix_to = min(unix_to, unix_now)
        unix_from = min(unix_from, unix_to)
        if unix_to == unix_from: return []
        
        # Extend scope
        if min_chunk: target = (lower_whole(unix_from, min_chunk), min(upper_whole(unix_to, min_chunk), unix_now))
        else: target = (unix_from, unix_to)
        
        spans = kv_storage.get_or_set(key, [], list[tuple[float, float]])
        covered: tuple[float,float]|None = None
        for span_from, span_to in CachedSeriesDescriptor.missing_spans(spans, target): #get unfilled spans
            for start, end in CachedSeriesDescriptor.break_span((span_from, span_to), max_chunk): #break based on max chunk
                if end < unix_now or self.should_refresh(instance, start, end, *args):
                    ks_storage.set(key, self.func(instance, span_from, span_to, *args))
                    covered = (covered[0], end) if covered else (start, end)
        
        if covered:
            newspans = self.cover_spans(spans, covered)
            while spans != newspans and not kv_storage.compare_and_set(key, newspans, spans):
                spans = kv_storage.get(key, list[tuple[float,float]])
                newspans = self.cover_spans(spans, covered)
        
        return ks_storage.get(key, unix_from, unix_to)
    
    def invalidate(self, instance: S, unix_from: float, unix_to: float, *args: *Args):
        key = self.get_key(instance, *args)
        kv_storage = self.get_kv_storage(instance, *args)
        spans = kv_storage.get_or_set(key, [], list[tuple[float,float]])
        newspans = self.remove_span(spans, (unix_from, unix_to))
        while spans != newspans and not kv_storage.compare_and_set(key, newspans, spans):
            spans = kv_storage.get(key, list[tuple[float,float]])
            newspans = self.remove_span(spans, (unix_from, unix_to))

    @overload
    def __get__(self, instance: None, owner: type[S]) -> Self: ...
    @overload
    def __get__(self, instance: S, owner: type[S]) -> Callable[[float, float, *Args], Sequence[T]]: ...
    def __get__(self, instance: S|None, owner: type[S]) -> Callable[[float, float, *Args], Sequence[T]]|Self:
        if instance is None: return self
        else: return lambda unix_from, unix_to, *args: self.cached_method(instance, unix_from, unix_to, *args)

    @staticmethod
    def missing_spans(existing: Sequence[tuple[float,float]], target: tuple[float, float]) -> Iterable[tuple[float,float]]:
        i = binary_search(existing, target[0], key=lambda it: it[0], side='LE')
        j = binary_search(existing, target[1], key=lambda it: it[1], side='GE')
        if i < 0 or existing[i][1] <= target[0]: i += 1
        if j == len(existing) or existing[j][0] >= target[1]: j -= 1
        spans = existing[i:j+1]
        if not spans:
            yield target
            return
        for i in range(len(spans)+1):
            if i == 0:
                if spans[0][0] > target[0]: yield (target[0], spans[0][0])
            if i > 0 and i < len(spans):
                yield (spans[i-1][1], spans[i][0])
            if i == len(spans):
                if spans[-1][1] < target[1]: yield (spans[-1][1], target[1])

    @staticmethod
    def break_span(target: tuple[float, float], max_chunk: float|None) -> Iterable[tuple[float,float]]:
        if not max_chunk:
            yield target
            return
        start = target[0]
        while start + max_chunk <= target[1]:
            yield (start, start+max_chunk)
            start = start+max_chunk
        if start < target[1]:
            yield (start, target[1])
    
    @staticmethod
    def cover_spans(existing: Sequence[tuple[float,float]], covered: tuple[float,float]) -> list[tuple[float,float]]:
        if covered[0] >= covered[1]: return list(existing)
        i = binary_search(existing, covered[0], key=lambda it: it[0], side='LE')
        j = binary_search(existing, covered[1], key=lambda it: it[1], side='GE')
        if i < 0 or existing[i][1] < covered[0]: i += 1
        else: covered = (existing[i][0], covered[1])
        if j == len(existing) or existing[j][0] > covered[1]: j -= 1
        else: covered = (covered[0], existing[j][1])
        return [*existing[:i], covered, *existing[j+1:]]
    
    @staticmethod
    def remove_span(existing: Sequence[tuple[float,float]], target: tuple[float,float]) -> list[tuple[float,float]]:
        i = binary_search(existing, target[0], key=lambda it: it[0], side='LT')
        j = binary_search(existing, target[1], key=lambda it: it[1], side='GT')
        result = []
        if i >= 0:
            result.extend(existing[:i])
            if existing[i][1] > target[0]: result.append((existing[i][0], target[0]))
            else : result.append(existing[i])
        if j < len(existing):
            if existing[j][0] < target[1]: result.append((target[1], existing[j][1]))
            else: result.append(existing[j])
            result.extend(existing[j+1:])
        return result

def cached_series(
    *,
    key: Callable[[S, *Args], str] = lambda self, *args: "_".join(str(it) for it in args),
    kv_storage: KeyValueStorage | Callable[[S, *Args], KeyValueStorage],
    ks_storage: KeySeriesStorage[T] | Callable[[S, *Args], KeySeriesStorage[T]],
    min_chunk: float | None | Callable[[S, *Args], float|None] = None,
    max_chunk: float | None | Callable[[S, *Args], float|None] = None,
    live_delay: float | Callable[[S, *Args], float] = 0,
    should_refresh: float | Callable[[S, float, float, *Args], bool] = 0,
) -> Callable[[Callable[[S, float, float, *Args], Sequence[T]]], CachedSeriesDescriptor[S, *Args, T]]:
    def decorate(func: Callable[[S, float, float, *Args], Sequence[T]]) -> CachedSeriesDescriptor[S, *Args, T]:
        return CachedSeriesDescriptor(
            func,
            key,
            kv_storage if callable(kv_storage) else (lambda self, *args: cast(KeyValueStorage, kv_storage)),
            ks_storage if callable(ks_storage) else (lambda self, *args: cast(KeySeriesStorage[T], ks_storage)),
            min_chunk if callable(min_chunk) else (lambda self, *args: cast(float|None, min_chunk)),
            max_chunk if callable(max_chunk) else (lambda self, *args: cast(float|None, max_chunk)),
            live_delay if callable(live_delay) else (lambda self, *args: -1.0e10) if live_delay is None else (lambda self, *args: cast(float, live_delay)),
            should_refresh if callable(should_refresh) else lambda self, fetch, now, *args: now-fetch > cast(float, should_refresh)
        )
    return decorate
