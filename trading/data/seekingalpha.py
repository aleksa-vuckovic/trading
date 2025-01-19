from ..utils import httputils
import json
import logging
from datetime import datetime
from pathlib import Path
from ..utils import common

_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = Path(__file__).parent / 'cache'

@common.backup_timeout()
def _get_news(ticker: str, unix_from: int, unix_to: int, *, logger: logging.Logger = None) -> list[dict]:
    ticker = ticker.lower()
    url = f"https://seekingalpha.com/api/v3/symbols/{ticker}/news?filter[since]={unix_from}&filter[until]={unix_to}&id={ticker}&include=author&isMounting=true&page[size]=50&page[number]="
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
    return ret
    
def get_news(ticker: str, unix_from: float, unix_to: float, *, logger: logging.Logger = None) -> list[str]:
    path = _CACHE / _MODULE / ticker.lower()
    path.mkdir(parents = True, exist_ok = True)
    PERIOD = 100000000
    from_id = int(unix_from) // PERIOD
    to_id = int(unix_to) // PERIOD
    now_id = int()
    ret = []
    for i in range(from_id, to_id+1):
        subpath = path / str(i)
        if subpath.exists():
            ret.extend(json.loads(subpath.read_text()))
        else:
            newdata = _get_news(ticker, i*PERIOD, (i+1)*PERIOD, logger = logger)
            newdata.sort(key = lambda value: value['unix_time'])
            subpath.write_text(json.dumps(newdata))
            ret.extend(newdata)
    if not ret:
        return ret
    start = 0
    while start < len(ret) and ret[start]['unix_time'] < unix_from:
        start += 1
    end = len(ret) -1
    while end >= 0 and ret[end]['unix_time'] > unix_to:
        end -= 1
    return [it['title'] for it in ret[start:end+1]]