#3
from __future__ import annotations
import logging
import time
from typing import Callable, override, ParamSpec, TypeVar, Sequence
from base import dates
from trading.core import Interval
from trading.core.securities import Security, DataProvider
from trading.core.pricing import PricingProvider, OHLCV
from trading.core.news import News, NewsProvider
from trading.providers.yahoo import Yahoo
from trading.providers.financialtimes import FinancialTimes
from trading.providers.wallstreetjournal import WallStreetJournal
from trading.providers.seekingalpha import SeekingAlpha
from trading.providers.globenewswire import GlobeNewswire

logger = logging.getLogger(__name__)

P = ParamSpec('P')
T = TypeVar('T')

class AggregateProvider(PricingProvider, NewsProvider, DataProvider):
    def __init__(self, pricing_providers: Sequence[PricingProvider], news_providers: Sequence[NewsProvider], data_providers: Sequence[DataProvider]):
        self.pricing_providers = pricing_providers
        self.news_providers = news_providers
        self.data_providers = data_providers
    def _delegate_call(self, methods: Sequence[Callable[P, T]], *args: P.args, **kwargs: P.kwargs) -> T:
        for i,method in enumerate(methods):
            try:
                return method(*args, **kwargs)
            except:
                logger.warning(f"Failed to invoke {method.__qualname__}.", exc_info=True)
                if i == len(methods)-1: raise
        raise Exception("No methods to invoke.")

    @override
    def get_pricing(self, unix_from: float, unix_to: float, security: Security, interval: Interval, *, interpolate: bool = False, max_fill_ratio: float = 1) -> Sequence[OHLCV]:
        try:
            return self.pricing_providers[0].get_pricing(unix_from, unix_to, security, interval, interpolate=interpolate, max_fill_ratio=max_fill_ratio)
        except:
            if unix_to < dates.unix() - 4*24*3600 or interval > Interval.D1: raise
            sep = max(dates.unix() - 4*24*3600, unix_from)
            if unix_from < sep:
                old = self.pricing_providers[0].get_pricing(unix_from, sep, security, interval, interpolate=interpolate, max_fill_ratio=max_fill_ratio)
            else:
                old = None
            recent = self._delegate_call([it.get_pricing for it in self.pricing_providers], unix_from, unix_to, security, interval, interpolate=interpolate, max_fill_ratio=max_fill_ratio)
            if old:
                result = list(old)
                result.extend(recent)
                return result
            else:
                return recent
            
    @override
    def get_interval_start(self, interval: Interval) -> float:
        return min([it.get_interval_start(interval) for it in self.pricing_providers])
    
    @override
    def get_news(self, unix_from: float, unix_to: float, security: Security) -> Sequence[News]:
        return self._delegate_call([it.get_news for it in self.news_providers], unix_from, unix_to, security)
    
    @override
    def get_outstanding_parts(self, security: Security) -> float:
        return self._delegate_call([it.get_outstanding_parts for it in self.data_providers], security)
    @override
    def get_market_cap(self, security: Security) -> float:
        return self._delegate_call([it.get_market_cap for it in self.data_providers], security)
    @override
    def get_first_trade_time(self, security: Security) -> float:
        return self._delegate_call([it.get_first_trade_time for it in self.data_providers], security)
    @override
    def get_summary(self, security: Security) -> str:
        return self._delegate_call([it.get_summary for it in self.data_providers], security)
    
    instance: AggregateProvider

pricing_providers: list[PricingProvider] = [Yahoo(), WallStreetJournal(), FinancialTimes()]
news_providers: list[NewsProvider] = [GlobeNewswire(), SeekingAlpha()]
data_providers: list[DataProvider] = [Yahoo()]

AggregateProvider.instance = AggregateProvider(pricing_providers, news_providers, data_providers)