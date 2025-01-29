from ..utils import httputils
import json
import logging
from datetime import datetime
from pathlib import Path
from ..utils import common

_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = common.CACHE / _MODULE

@common.cached_series(
    cache_root=_CACHE,
    unix_from_arg=1,
    unix_to_arg=2,
    include_args=0,
    time_step_fn=100000000,
    series_field=None,
    timestamp_field="unix_time",
    live_delay=12*3600
)
@common.backup_timeout(behavior=common.BackupBehavior.RETHROW)
def _get_news(ticker: str, unix_from: float, unix_to: float) -> list[dict]:
    ticker = ticker.lower()
    url = f"https://seekingalpha.com/api/v3/symbols/{ticker}/news?filter[since]={int(unix_from)}&filter[until]={int(unix_to)}&id={ticker}&include=author&isMounting=true&page[size]=50&page[number]="
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
    return sorted([it for it in ret if it['unix_time'] >= unix_from and it['unix_time'] < unix_to], key = lambda it: it['unix_time'])
    
def get_news(ticker: str, unix_from: float, unix_to: float) -> list[str]:
    return [it['title'] for it in _get_news(ticker.upper(), unix_from, unix_to)]