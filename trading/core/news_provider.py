from typing import Sequence, override
from base.caching import cached_series, Persistor
from trading.core.securities import NewsProvider, Security

class BaseNewsProvider(NewsProvider):
    
    @override
    def get_news(self, unix_from: float, unix_to: float, security: Security) -> list[dict]:
        return self._get_news(unix_from, unix_to, security)
    @override
    def get_titles(self, unix_from: float, unix_to: float, security: Security) -> Sequence[str]:
        return [it['title'] for it in self.get_news(unix_from, unix_to, security)]

    @staticmethod
    def _get_news_timestamp_fn(it: dict) -> float: return it['unix_time']
    def _get_news_key_fn(self, security: Security) -> str: return f"{security.exchange.mic}_{security.symbol}"
    def _get_news_persistor_fn(self, security: Security) -> Persistor: return self.get_news_persistor(security)
    @cached_series(
        timestamp_fn=_get_news_timestamp_fn,
        key_fn=_get_news_key_fn,
        persistor_fn=_get_news_persistor_fn,
        time_step_fn=10000000,
        live_delay_fn=3600, #let's say that news is an hour late usually
        should_refresh_fn=2*3600
    )
    def _get_news(self, unix_from: float, unix_to: float, security: Security) -> list[dict]:
        return self.get_news_raw(unix_from, unix_to, security)

    #region Abstract
    def get_news_persistor(self, security: Security) -> Persistor: raise NotImplementedError()
    def get_news_raw(self, unix_from: float, unix_to: float, security: Security) -> list[dict]: raise NotImplementedError()
    #endregion