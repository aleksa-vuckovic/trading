#2
from typing import Sequence, override
from base.key_value_storage import SqlKVStorage, KeyValueStorage, MongoKVStorage
from base.key_series_storage import SqlKSStorage, KeySeriesStorage, MongoKSStorage
from base.caching import cached_series
from base.serialization import Serializable
import injection
from trading.core.pricing import MongoKVStorage
from trading.core.securities import Security

class News(Serializable):
    def __init__(self, unix_time: float, title: str, content: str|None):
        self.unix_time = unix_time
        self.title = title
        self.content = content

    def __repr__(self) -> str:
        return f"News(unix_time={self.unix_time},title='{self.title}',content='{self.content}')"

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
    def __init__(self):
        name = type(self).__name__.lower()
        self.local_news_storage = (SqlKVStorage(injection.local_db, f"{name}_news_span"), SqlKSStorage[News](injection.local_db, f"{name}_news", lambda it: it.unix_time))
        self.remote_news_storage = (MongoKVStorage(injection.mongo_db[f"{name}_news_span"]), MongoKSStorage[News](injection.mongo_db[f"{name}_news"], lambda it: it.unix_time))

    @override
    def get_news(self, unix_from: float, unix_to: float, security: Security) -> Sequence[News]:
        return self._get_news(unix_from, unix_to, security)

    def _get_news_key(self, security: Security) -> str: return f"{security.exchange.mic}_{security.symbol}"
    def _get_news_kv_storage(self, security: Security) -> KeyValueStorage: return self.local_news_storage[0]
    def _get_news_ks_storage(self, security: Security) -> KeySeriesStorage[News]: return self.local_news_storage[1]
    @cached_series(
        key=_get_news_key,
        kv_storage=_get_news_kv_storage,
        ks_storage=_get_news_ks_storage,
        min_chunk=10000000,
        max_chunk=10000000,
        live_delay=3600, #let's say that news is an hour late usually
        should_refresh=2*3600
    )
    def _get_news(self, unix_from: float, unix_to: float, security: Security) -> Sequence[News]:
        return self.get_news_raw(unix_from, unix_to, security)

    #region Abstract
    def get_news_raw(self, unix_from: float, unix_to: float, security: Security) -> Sequence[News]: raise NotImplementedError()
    #endregion