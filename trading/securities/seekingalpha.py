import json
import logging
from datetime import datetime
from pathlib import Path
from trading.utils import httputils
from base.caching import cached_series, CACHE_ROOT, DB_PATH, Persistor, FilePersistor, SqlitePersistor
from trading.securities.utils import filter_by_timestamp
from trading.core.securities import Security, NewsProvider

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

class SeekingAlpha(NewsProvider):
    def __init__(self, use_file: bool = False):
        self.news_persistor = FilePersistor(CACHE_ROOT/_MODULE/'news') if use_file else SqlitePersistor(DB_PATH, f"{_MODULE}_news")

    def _get_news_key_fn(self, security: Security) -> list[str]:
        return [security.symbol.lower()]
    def _get_news_persistor_fn(self, security: Security) -> Persistor:
        return self.news_persistor
    @cached_series(
        unix_args=(2,3),
        series_field=None,
        timestamp_field="unix_time",
        key_fn=_get_news_key_fn,
        persistor_fn=_get_news_persistor_fn,
        time_step_fn=100000000,
        live_delay_fn=3600,
        should_refresh_fn=12*3600
    )
    @httputils.backup_timeout()
    def _fetch_news(self, symbol: str, unix_from: float, unix_to: float) -> list[dict]:
        url = f"https://seekingalpha.com/api/v3/symbols/{symbol}/news?filter[since]={int(unix_from-1000)}&filter[until]={int(unix_to+1000)}&id={symbol}&include=author&isMounting=true&page[size]=50&page[number]="
        i = 1
        ret = []
        while True:
            result = httputils.get_as_browser(url + str(i), cookies={'_px3': 'af68f8fb5b5ccac7de1bcd1e9557a64ca5b54a306698c0183f442b892e57d595:dKFZkKZdn5YMhnVe9/z6Mmeu0Vwm9hHxkSUA12xuWMNzrzBEhvWN72yAkcs/21uiP2FNRzAYmXdyl1IhWgTqig==:1000:XTwygXvwwmUUlYgT7A6afuaPKRfJfP1V/ubqBfweaOzdYj2FzrwVIEpS1yYWXwHLg2Hdu8M4pe1w6fKMCzre5OEMW3L/9/vFIfRzFIQtOXysmQuLcIU33BSX4fmq4wtEMaSHXZ70WNml3AY90zQCEDe/xiIN0Q6JcQNtnFJzkmwCeU3vqCzGsti1r4SuxMkeyr0MVTrVnwWee1wEWlPGdeB57SWQIZ+Dt71z86HzN7I='})
            data = json.loads(result.text)['data']
            for item in data:
                item = item['attributes']
                if 'title' in item and 'publishOn' in item:
                    try:
                        ret.append({
                            'title': item['title'], 
                            'unix_time': int(datetime.fromisoformat(item['publishOn']).timestamp())
                        })
                    except:
                        pass
            if len(data) < 50:
                break
            i += 1
        ret = filter_by_timestamp(ret, unix_from=unix_from, unix_to=unix_to, timestamp_field='unix_time')
        return sorted(ret, key = lambda it: it['unix_time'])
    def get_news(self, security, unix_from, unix_to, **kwargs):
        return [it['title'] for it in self._fetch_news(security.symbol, unix_from, unix_to, **kwargs)]