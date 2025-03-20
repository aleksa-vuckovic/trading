#2
from typing import Sequence, override
from base.caching import cached_series, Persistor
from trading.core import Interval
from trading.core.securities import PricingProvider, Security

def interpolate_linear(raw_data: Sequence[dict], timestamps: Sequence[float], timestamp_field = 't') -> list[dict]:
    """
    Interpolates linearly the raw data fields for all missing timestamps.
    The existing raw timestamps must match with the expected ones.
    The interpolation is linear and based on timestamps.
    The length of the returned list and its timestamps exactly match the timestamps argument.
    """
    t = timestamp_field
    if not raw_data:
        if timestamps: raise Exception(f"Can't interpolate with no data at all.")
        else: return []
    
    result = [] if raw_data[0][t] == timestamps[0] else [{**raw_data[0], t: timestamps[0]}]
    for entry in raw_data:
        if entry[t] == timestamps[len(result)]:
            result.append(entry)
        else:
            start = result[-1]
            while entry[t] > timestamps[len(result)]:
                factor = (timestamps[len(result)] - start[t])/(entry[t]-start[t])
                result.append({
                    key: (start[key]*(1-factor)+entry[key]*factor)
                    for key in start
                })
            result.append(entry)
    while len(result) < len(timestamps): result.append({**result[-1], t: timestamps[len(result)]})
    return result

def merge_pricing(
    data: Sequence[dict],
    unix_from: float,
    unix_to: float,
    interval: Interval,
    security: Security
) -> list[dict]:
    calendar = security.exchange.calendar
    result = []
    i = 0
    while i < len(data):
        t = data[i]['t']
        timestamp = calendar.get_next_timestamp(t, interval) if not calendar.is_timestamp(t, interval) else t
        if timestamp > unix_to: break
        j = i
        while j < len(data) and data[j]['t'] <= timestamp: j += 1
        seg = data[i:j]
        i = j
        if timestamp <= unix_from: continue
        result.append({
            't': timestamp,
            'o': seg[0]['o'],
            'h': max(it['h'] for it in seg),
            'l': min(it['l'] for it in seg),
            'c': seg[-1]['c'],
            'v': sum(it['v'] for it in seg)
        })
    return result
    
class BasePricingProvider(PricingProvider):
    def __init__(
        self,
        intervals: dict[Interval, Interval|None]
    ):
        """
        Args:
            intervals: Determines which intervals are supported by this provider (keys), and if those intervals are calculated
                based on a smaller one (value).
        """
        self.intervals = intervals
    
    @override
    def get_pricing(self, unix_from, unix_to, security, interval, *, return_quotes = ['close'], interpolate = False, max_fill_ratio = 1):
        return_quotes = [it[0].lower() for it in return_quotes]
        data = self._get_pricing(unix_from, unix_to, security, interval)
        if interpolate:
            timestamps = security.exchange.calendar.get_timestamps(unix_from, unix_to, interval)
            fill_ratio = (len(timestamps)-len(data))/len(timestamps) if timestamps else 0
            if fill_ratio > max_fill_ratio:
                raise Exception(f"Fill ratio {fill_ratio} is larger than the maximum {max_fill_ratio}.")
            data = interpolate_linear(data, timestamps)
        return tuple([it[quote] for it in data] for quote in return_quotes)

    @staticmethod
    def _get_pricing_timestamp_fn(it: dict): return it['t']
    def _get_pricing_key_fn(self, security: Security, interval: Interval) -> str:
        return f"{security.exchange.mic}_{security.symbol}_{interval.name}"
    def _get_pricing_persistor_fn(self, security: Security, interval: Interval) -> Persistor:
        return self.get_pricing_persistor(security, interval)
    def _get_pricing_time_step_fn(self, security: Security, interval: Interval) -> float:
        if interval == Interval.L1: return 1000000000
        elif interval == Interval.W1: return 300000000
        elif interval == Interval.D1: return 50000000
        elif interval == Interval.H1: return 10000000
        elif interval == Interval.M15: return 2000000
        elif interval == Interval.M5: return 1000000
        else: raise Exception(f"Unknown interval {interval}.")
    def _get_pricing_live_delay_fn(self, security: Security, interval: Interval) -> float:
        return self.get_pricing_delay(security, interval)
    def _get_pricing_should_refresh_fn(self, fetch: float, now: float, security: Security, interval: Interval) -> bool:
        return security.exchange.calendar.get_next_timestamp(fetch, interval) < now
    @cached_series(
        timestamp_fn=_get_pricing_timestamp_fn,
        key_fn=_get_pricing_key_fn,
        persistor_fn=_get_pricing_persistor_fn,
        time_step_fn=_get_pricing_time_step_fn,
        live_delay_fn=_get_pricing_live_delay_fn,
        should_refresh_fn=_get_pricing_should_refresh_fn
    )
    def _get_pricing(
        self,
        unix_from: float,
        unix_to: float,
        security: Security,
        interval: Interval,
        **kwargs
    ) -> list[dict]:
        if interval not in self.intervals:
            raise Exception(f"Unsupported interval {interval}. Supported intervals are {list(self.intervals.keys())}.")
        base_interval = self.intervals[interval]
        if not base_interval or unix_from <= self.get_interval_start(base_interval):
            return self.get_pricing_raw(unix_from, unix_to, security, interval)
        data = self._get_pricing(security.exchange.calendar.add_intervals(unix_from, interval, -1), unix_to, security, base_interval)
        return merge_pricing(data, unix_from, unix_to, interval, security)

    #region Abstract
    def get_interval_start(self, interval: Interval) -> float: raise NotImplementedError()
    def get_pricing_persistor(self, security: Security, interval: Interval) -> Persistor: raise NotImplementedError()
    def get_pricing_delay(self, security: Security, interval: Interval) -> float: raise NotImplementedError()
    def get_pricing_raw(self, unix_from: float, unix_to: float, security: Security, interval: Interval) -> list[dict]:
        """
        Implement this so that it fetches raw fresh data and returns a list of dicts containing quotes.
        The dict keys should be tohlcv.
        """
        raise NotImplementedError()
    #endregion
