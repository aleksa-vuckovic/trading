import logging
import time
from trading.core import Interval
from trading.securities import Yahoo, FinancialTimes, WallStreetJournal, SeekingAlpha, GlobeNewswire, PricingProvider, NewsProvider, DataProvider

logger = logging.getLogger(__name__)

pricing_providers: list[PricingProvider] = [Yahoo(), WallStreetJournal(), FinancialTimes()]
news_providers: list[NewsProvider] = [GlobeNewswire(), SeekingAlpha()]
data_providers: list[DataProvider] = [Yahoo()]

class AggregateProvider(PricingProvider, NewsProvider, DataProvider):

    def _delegate_call(self, name: str, providers: list, *args, **kwargs):
        for i in range(len(providers)):
            try:
                return getattr(providers, name)(*args, **kwargs)
            except:
                logger.warning(f"Failed to invoke {name} on {type(providers[i]).__name__}.", exc_info=True)
                if i == len(providers)-1: raise

    def get_pricing(self, security, unix_from, unix_to, interval, *, return_quotes = ..., interpolate = False, max_fill_ratio = 1, **kwargs):
        try:
            return pricing_providers[0].get_pricing(security, unix_from, unix_to, interval, return_quotes=return_quotes, interpolate=interpolate, max_fill_ratio=max_fill_ratio, **kwargs)
        except:
            if unix_to < time.time() - 4*24*3600 or interval > Interval.D1: raise
            sep = max(time.time() - 4*24*3600, unix_from)
            if unix_from < sep:
                old = pricing_providers[0].get_pricing(security, unix_from, sep, interval, return_quotes=return_quotes, interpolate=interpolate, max_fill_ratio=max_fill_ratio, **kwargs)
            else:
                old = None
            recent = self._delegate_call(PricingProvider.get_pricing.__name__, pricing_providers, security, unix_from, unix_to, interval, return_quotes=return_quotes, interpolate=interpolate, max_fill_ratio=max_fill_ratio, **kwargs)
            if old:
                for i in range(len(recent)): old[i].extend(recent[i])
                return old
            else:
                return recent
    
    def get_news(self, security, unix_from, unix_to, **kwargs):
        return self._delegate_call(NewsProvider.get_news.__name__, news_providers, security, unix_from, unix_to, **kwargs)
    
    def get_outstanding_parts(self, security):
        return self._delegate_call(DataProvider.get_outstanding_parts.__name__, data_providers, security)
    def get_market_cap(self, security):
        return self._delegate_call(DataProvider.get_market_cap.__name__, data_providers, security)
    def get_first_trade_time(self, security):
        return self._delegate_call(DataProvider.get_first_trade_time.__name__, data_providers, security)
    def get_summary(self, security):
        return self._delegate_call(DataProvider.get_summary.__name__, data_providers, security)