import logging
import time
from typing import Callable, override, ParamSpec, TypeVar, Sequence
from trading.core import Interval
from trading.core.securities import Security
from trading.providers import Yahoo, FinancialTimes, WallStreetJournal, SeekingAlpha,GlobeNewswire, PricingProvider, NewsProvider, DataProvider

logger = logging.getLogger(__name__)

pricing_providers: list[PricingProvider] = [Yahoo(), WallStreetJournal(), FinancialTimes()]
news_providers: list[NewsProvider] = [GlobeNewswire(), SeekingAlpha()]
data_providers: list[DataProvider] = [Yahoo()]

P = ParamSpec('P')
T = TypeVar('T')

class AggregateProvider(PricingProvider, NewsProvider, DataProvider):

    def _delegate_call(self, methods: Sequence[Callable[P, T]], *args: P.args, **kwargs: P.kwargs) -> T:
        for i,method in enumerate(methods):
            try:
                return method(*args, **kwargs)
            except:
                logger.warning(f"Failed to invoke {method.__qualname__}.", exc_info=True)
                if i == len(methods)-1: raise
        raise Exception("No methods to invoke.")

    @override
    def get_pricing(self, unix_from: float, unix_to: float, security: Security, interval: Interval, *, return_quotes: list[str], interpolate: bool, max_fill_ratio: float) -> tuple[list[float], ...]:
        try:
            return pricing_providers[0].get_pricing(unix_from, unix_to, security, interval, return_quotes=return_quotes, interpolate=interpolate, max_fill_ratio=max_fill_ratio)
        except:
            if unix_to < time.time() - 4*24*3600 or interval > Interval.D1: raise
            sep = max(time.time() - 4*24*3600, unix_from)
            if unix_from < sep:
                old = pricing_providers[0].get_pricing(unix_from, sep, security, interval, return_quotes=return_quotes, interpolate=interpolate, max_fill_ratio=max_fill_ratio)
            else:
                old = None
            recent = self._delegate_call([it.get_pricing for it in pricing_providers], unix_from, unix_to, security, interval, return_quotes=return_quotes, interpolate=interpolate, max_fill_ratio=max_fill_ratio)
            if old:
                for i in range(len(recent)): old[i].extend(recent[i])
                return old
            else:
                return recent
    
    @override
    def get_news(self, unix_from: float, unix_to: float, security: Security) -> Sequence[dict]:
        return self._delegate_call([it.get_news for it in news_providers], unix_from, unix_to, security)
    
    @override
    def get_outstanding_parts(self, security: Security) -> float:
        return self._delegate_call([it.get_outstanding_parts for it in data_providers], security)
    @override
    def get_market_cap(self, security: Security) -> float:
        return self._delegate_call([it.get_market_cap for it in data_providers], security)
    @override
    def get_first_trade_time(self, security: Security) -> float:
        return self._delegate_call([it.get_first_trade_time for it in data_providers], security)
    @override
    def get_summary(self, security: Security) -> str:
        return self._delegate_call([it.get_summary for it in data_providers], security)
    