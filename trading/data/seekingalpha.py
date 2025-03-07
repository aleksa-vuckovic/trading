import json
import logging
from datetime import datetime
from pathlib import Path
from ..utils import httputils
from .caching import cached_series, CACHE_ROOT, FilePersistor
from .utils import filter_by_timestamp
from .abstract import AbstractSecurity

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = CACHE_ROOT / _MODULE

def _get_news_key_fn(security: AbstractSecurity) -> list[str]:
    return [security.symbol.lower()]
@cached_series(
    unix_args=(1,2),
    series_field=None,
    timestamp_field="unix_time",
    key_fn=_get_news_key_fn,
    persistor_fn=FilePersistor(CACHE_ROOT/_MODULE/'news'),
    time_step_fn=100000000,
    live_delay_fn=3600,
    should_refresh_fn=12*3600
)
@httputils.backup_timeout()
def _get_news(security: AbstractSecurity, unix_from: float, unix_to: float) -> list[dict]:
    ticker = security.symbol.lower()
    url = f"https://seekingalpha.com/api/v3/symbols/{ticker}/news?filter[since]={int(unix_from-1000)}&filter[until]={int(unix_to+1000)}&id={ticker}&include=author&isMounting=true&page[size]=50&page[number]="
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
def get_news(security: AbstractSecurity, unix_from: float, unix_to: float, **kwargs) -> list[str]:
    return [it['title'] for it in _get_news(security, unix_from, unix_to, **kwargs)]