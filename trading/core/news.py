#2
from typing import Sequence, override
from base.caching import cached_series, Persistor
from base.serialization import Serializable
from trading.core.securities import Security

class News(Serializable):
    def __init__(self, time: float, title: str, content: str):
        self.time = time
        self.title = title
        self.content = content

    def __repr__(self) -> str:
        return f"News(time={self.time},title='{self.title}',content='{self.content}')"

class NewsProvider:
    """
    News providers will:
        1. Raise an exception if info for a given security is not available.
        2. Ignore interval parts that are unavailable.
    """
    def get_news(
        self,
        unix_from: float,
        unix_to: float,
        security: Security
    ) -> Sequence[News]:
        raise NotImplementedError()

class BaseNewsProvider(NewsProvider):
    
    @override
    def get_news(self, unix_from: float, unix_to: float, security: Security) -> list[News]:
        return self._get_news(unix_from, unix_to, security)

    @staticmethod
    def _get_news_timestamp_fn(it: News) -> float: return it.time
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
    def _get_news(self, unix_from: float, unix_to: float, security: Security) -> Sequence[News]:
        return self.get_news_raw(unix_from, unix_to, security)

    #region Abstract
    def get_news_persistor(self, security: Security) -> Persistor: raise NotImplementedError()
    def get_news_raw(self, unix_from: float, unix_to: float, security: Security) -> Sequence[News]: raise NotImplementedError()
    #endregion