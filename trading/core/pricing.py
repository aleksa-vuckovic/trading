#2
from __future__ import annotations
import math
from typing import Iterable, Mapping, Sequence, overload, override
from base.key_value_storage import SqlKVStorage, MongoKVStorage, MemoryKVStorage
from base.key_series_storage import SqlKSStorage, MongoKSStorage, MemoryKSStorage
from base.algos import interpolate
from base.types import Equatable
from base.serialization import Serializable
from base.caching import cached_series
import injection
from trading.core import Interval
from trading.core.securities import Security

class OHLCV(Equatable, Serializable):
    def __init__(self, t: float, o: float, h: float, l: float, c: float, v: float):
        self.t = t
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.v = v

    def __getitem__(self, key: str) -> float:
        return self.__dict__[key[0].lower()]
    def __setitem__(self, key: str, value: float):
        self.__dict__[key[0].lower()] = value

    def __repr__(self) -> str:
        return f"OHLCV(t={self.t},o={self.o},h={self.h},l={self.l},c={self.c},v={self.v})"

    def adjust(self, factor: float) -> OHLCV:
        self.o *= factor
        self.h *= factor
        self.l *= factor
        self.c *= factor
        self.v *= 1/factor
        return self
    
    def is_valid(self) -> bool:
        if any(self[key]<0 for key in 'ohlcv'): return False
        if any(self[key]==0 for key in 't'): return False
        if any(math.isnan(self[key]) or math.isinf(self[key]) for key in 'tohlcv'): return False
        #if any(self[key] < self.l for key in 'ohc'): return False
        #if any(self[key] > self.h for key in 'olc'): return False
        return True

    @staticmethod
    def interpolate(data: Sequence[OHLCV], timestamps: Sequence[float]) -> list[OHLCV]:
        x = [it.t for it in data]
        result: dict[str, Sequence[float]] = {
            key: interpolate(x, [it[key] for it in data], timestamps, method='linear_edge')
            for key in 'ohlcv'
        }
        return [OHLCV(timestamps[i], *(result[key][i] for key in 'ohlcv')) for i in range(len(timestamps))]

class PricingProvider:
    """
    Pricing providers will:
        1. Raise an exception if info for a given security is not available.
        2. Raise an exception if the interval is not supported.
        3. Ignore interval parts that are unavailable.
    """
    #region Abstract
    def get_pricing(
        self,
        unix_from: float,
        unix_to: float,
        security: Security,
        interval: Interval,
        *,
        interpolate: bool = False,
        max_fill_ratio: float = 1
    ) -> Sequence[OHLCV]:
        """
        Returns the requested pricing data fresh from the source.
        Args:
            interpolate: If true, will return values for all interval timestamps, interpolating with known values if necessary.
            max_fill_ratio: Max ratio of (number of missing or interpolated entries)/(total number of entries).
                Only used when interpolate=True.
        """
        raise NotImplementedError()
    def get_intervals(self) -> set[Interval]:
        """
        Get all intervals supported by this pricing provider.
        """
        raise NotImplementedError()
    def get_interval_start(self, interval: Interval) -> float:
        """
        Get the unix timestamp of the start-of-availability for the given interval.
        """
        raise NotImplementedError()
    #endregion

    def get_pricing_at(self, unix_time: float, security: Security, interval: Interval = Interval.M1) -> float:
        unix_from = security.exchange.calendar.add_intervals(unix_time, interval, -1)
        p = self.get_pricing(unix_from, unix_time, security, interval, interpolate=True)[-1]
        return (p.o+p.c)/2

def merge_pricing(
    data: Sequence[OHLCV],
    unix_from: float,
    unix_to: float,
    interval: Interval,
    security: Security
) -> list[OHLCV]:
    calendar = security.exchange.calendar
    result: list[OHLCV] = []
    i = 0
    while i < len(data):
        t = data[i].t
        timestamp = calendar.get_next_timestamp(t, interval) if not calendar.is_timestamp(t, interval) else t
        if timestamp > unix_to: break
        j = i
        while j < len(data) and data[j].t <= timestamp: j += 1
        seg = data[i:j]
        i = j
        if timestamp <= unix_from: continue
        result.append(OHLCV(
            t = timestamp,
            o = seg[0].o,
            h = max(it.h for it in seg),
            l = min(it.l for it in seg),
            c = seg[-1].c,
            v = sum(it.v for it in seg)
        ))
    return result
    
class BasePricingProvider(PricingProvider):
    DEFAULT_MERGE = {
        Interval.H1: Interval.M30,
        Interval.M30: Interval.M15,
        Interval.M15: Interval.M5,
        Interval.M5: Interval.M1
    }
    def __init__(
        self,
        *,
        native: Iterable[Interval],
        merge: Mapping[Interval, Interval] = DEFAULT_MERGE,
        local: bool = False
    ):
        """
        Args:
            intervals: Determines which intervals are supported by this provider (keys), and if those intervals are calculated
                based on a smaller one (value).
        """
        self.native = set(native)
        self.merge = merge
        name = type(self).__name__.lower()
        if local:
            self.local_pricing_storage = (MemoryKVStorage(), MemoryKSStorage[OHLCV](lambda it: it.t))
            self.remote_pricing_storage = (MemoryKVStorage(), MemoryKSStorage[OHLCV](lambda it: it.t))
        else:
            self.local_pricing_storage = (SqlKVStorage(injection.local_db, f"{name}_pricing_span"), SqlKSStorage[OHLCV](injection.local_db, f"{name}_pricing", lambda it: it.t))
            self.remote_pricing_storage = (MongoKVStorage(injection.mongo_db[f"{name}_pricing_span"]), MongoKSStorage[OHLCV](injection.mongo_db[f"{name}_pricing"], lambda it: it.t))
    
    @override
    def get_pricing(self, unix_from, unix_to, security, interval, *, interpolate = False, max_fill_ratio = 1) -> Sequence[OHLCV]:
        data = self._get_pricing(unix_from, unix_to, security, interval)
        if interpolate:
            timestamps = security.exchange.calendar.get_timestamps(unix_from, unix_to, interval)
            fill_ratio = (len(timestamps)-len(data))/len(timestamps) if timestamps else 0
            if fill_ratio > max_fill_ratio:
                raise Exception(f"Fill ratio {fill_ratio} is larger than the maximum {max_fill_ratio}.")
            data = OHLCV.interpolate(data, timestamps)
        return data
    @override
    def get_intervals(self) -> set[Interval]:
        return self.native.union(self.merge.keys())

    #region caching
    def _get_pricing_key(self, security: Security, interval: Interval) -> str:
        return f"{security.exchange.mic}_{security.symbol}_{interval.name}"
    def _get_pricing_local_kv(self): return self.local_pricing_storage[0]
    def _get_pricing_local_ks(self): return self.local_pricing_storage[1]
    def _get_pricing_remote_kv(self): return self.remote_pricing_storage[0]
    def _get_pricing_remote_ks(self): return self.remote_pricing_storage[1]
    def _get_pricing_min_chunk(self, security: Security, interval: Interval) -> float:
        if interval == Interval.L1: return 1000000000
        elif interval == Interval.W1: return 300000000
        elif interval == Interval.D1: return 50000000
        elif interval == Interval.H1: return 12000000
        elif interval == Interval.M30: return 6000000
        elif interval == Interval.M15: return 3000000
        elif interval == Interval.M5: return 1000000
        elif interval == Interval.M1: return 200000
        else: raise Exception(f"Unknown interval {interval}.")
    def _get_pricing_live_delay(self, security: Security, interval: Interval) -> float:
        return self.get_pricing_delay(security, interval)
    def _get_pricing_should_refresh(self, fetch: float, now: float, security: Security, interval: Interval) -> bool:
        return security.exchange.calendar.get_next_timestamp(fetch, interval) < now and now-fetch > 3600
    @cached_series(
        key=_get_pricing_key,
        kv_storage=_get_pricing_remote_kv,
        ks_storage=_get_pricing_remote_ks,
        min_chunk=_get_pricing_min_chunk,
        max_chunk=_get_pricing_min_chunk,
        live_delay=_get_pricing_live_delay,
        should_refresh=_get_pricing_should_refresh
    )
    def _get_pricing_remote(
        self,
        unix_from: float,
        unix_to: float,
        security: Security,
        interval: Interval
    ) -> Sequence[OHLCV]:
        if interval not in self.native and interval not in self.merge:
            raise Exception(f"Unsupported interval {interval}. Supported intervals are {self.native.union(self.merge.keys())}.")
        if interval in self.merge and not(interval in self.native and unix_from < self.get_interval_start(self.merge[interval])):
            data = self._get_pricing(security.exchange.calendar.add_intervals(unix_from, interval, -1), unix_to, security, self.merge[interval])
            return merge_pricing(data, unix_from, unix_to, interval, security)
        else:
            return self.get_pricing_raw(unix_from, unix_to, security, interval)
    @cached_series(
        key=_get_pricing_key,
        kv_storage=_get_pricing_local_kv,
        ks_storage=_get_pricing_local_ks,
        live_delay=_get_pricing_live_delay,
    )
    def _get_pricing(
        self,
        unix_from: float,
        unix_to: float,
        security: Security,
        interval: Interval
    ) -> Sequence[OHLCV]:
        return self._get_pricing_remote(unix_from, unix_to, security, interval)
    
    @overload
    def invalidate_pricing(self, unix_from: float, unix_to: float): ...
    @overload
    def invalidate_pricing(self, unix_from: float, unix_to: float, security: Security, interval: Interval): ...
    def invalidate_pricing(self, unix_from: float, unix_to: float, security: Security|None=None, interval: Interval|None=None):
        if security and interval:
            BasePricingProvider._get_pricing_remote.invalidate(self, unix_from, unix_to, security, interval)
            BasePricingProvider._get_pricing.invalidate(self, unix_from, unix_to, security, interval)
        else:
            BasePricingProvider._get_pricing_remote.invalidate_all(self, unix_from, unix_to)
            BasePricingProvider._get_pricing_remote.invalidate_all(self, unix_from, unix_to)
    #endregion

    #region Abstract
    def get_pricing_delay(self, security: Security, interval: Interval) -> float: raise NotImplementedError()
    def get_pricing_raw(self, unix_from: float, unix_to: float, security: Security, interval: Interval) -> Sequence[OHLCV]:
        """
        Implement this so that it fetches raw fresh data and returns a list of dicts containing quotes.
        The dict keys should be tohlcv.
        """
        raise NotImplementedError()
    #endregion
