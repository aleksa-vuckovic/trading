#2
from base.caching import cached_series, Persistor
from trading.core import Interval
from trading.core.securities import PricingProvider, Security

def interpolate_linear(raw_data: list[dict], timestamps: list[float], timestamp_field = 't') -> list[dict]:
    if not raw_data:
        if timestamps: raise Exception(f"Can't interpolate with no data at all.")
        else: return []
    result = []
    raw_data.insert(0, raw_data[0]) # Pad to the left
    if raw_data[-1][timestamp_field] < timestamps[-1]: # Pad to the right if necessary
        raw_data.append({key:(value if key != timestamp_field else timestamps[-1]) for key,value in raw_data[-1].items()})
    j = 0
    for i in range(1,len(raw_data)):
        fills = 0
        while raw_data[i][timestamp_field] > timestamps[j]:
            fills += 1
            j += 1
        assert raw_data[i][timestamp_field] == timestamps[j]
        for r in range(fills+1):
            factor = (r+1)/(fills+1)
            timestamp = timestamps[j-fills+r]
            result.append({
                key: (raw_data[i-1][key]*(1-factor)+raw_data[i][key]*factor) if key != timestamp_field else timestamp
                for key in raw_data[i]
            })
        j+=1
    return result

def merge_pricing(
    data: list[dict],
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
        intervals: dict[Interval, Interval]
    ):
        self.intervals = intervals

    def get_pricing(self, security, unix_from, unix_to, interval, *, return_quotes = ..., interpolate = False, max_fill_ratio = 1, **kwargs):
        return_quotes = [it[0].lower() for it in return_quotes]
        data = self._get_pricing(security, unix_from, unix_to, interval, **kwargs)
        if interpolate:
            timestamps = security.exchange.calendar.get_timestamps(unix_from, unix_to, interval)
            fill_ratio = (len(timestamps)-len(data))/len(timestamps) if timestamps else 0
            if fill_ratio > max_fill_ratio:
                raise Exception(f"Fill ratio {fill_ratio} is larger than the maximum {max_fill_ratio}.")
            data = interpolate_linear(data, timestamps)
        return tuple([it[quote] for it in data] for quote in return_quotes)

    def _get_pricing_key_fn(self, security: Security, interval: Interval) -> list[str]:
        return [f"{security.exchange.mic}_{security.symbol}", interval.name]
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
    def _get_pricing_should_refresh_fn(self, security: Security, interval: Interval, fetch: float, now: float) -> bool:
        return security.exchange.calendar.get_next_timestamp(fetch, interval) < now
    @cached_series(
        unix_args=(2,3),
        series_field=None,
        timestamp_field='t',
        key_fn=_get_pricing_key_fn,
        persistor_fn=_get_pricing_persistor_fn,
        time_step_fn=_get_pricing_time_step_fn,
        live_delay_fn=_get_pricing_live_delay_fn,
        should_refresh_fn=_get_pricing_should_refresh_fn
    )
    def _get_pricing(
        self,
        security: Security,
        unix_from: float,
        unix_to: float,
        interval: Interval,
        **kwargs
    ) -> list[dict]:
        if interval not in self.intervals:
            raise Exception(f"Unsupported interval {interval}. Supported intervals are {list(self.intervals.keys())}.")
        base_interval = self.intervals[interval]
        if not base_interval or unix_from <= self.get_interval_start(base_interval):
            return self.get_pricing_raw(security, unix_from, unix_to, interval, **kwargs)
        base_interval = self.intervals[interval]
        data = self._get_pricing(security, security.exchange.calendar.add_intervals(unix_from, -1, interval), unix_to, base_interval, **kwargs)
        return merge_pricing(data, unix_from, unix_to, interval, security)

    def get_interval_start(self, interval: Interval) -> float:
        raise NotImplementedError()
    def get_pricing_persistor(self, security: Security, interval: Interval) -> Persistor:
        raise NotImplementedError()
    def get_pricing_delay(self, security: Security, interval: Interval) -> float:
        raise NotImplementedError()
    def get_pricing_raw(
        self,
        security: Security,
        unix_from: float,
        unix_to: float,
        interval: Interval,
        **kwargs
    ) -> list[dict]:
        """
        Implement this so that it fetches raw fresh data and returns a list of dicts containing quotes.
        The dict keys should be tohlcv.
        """
        raise NotImplementedError()
