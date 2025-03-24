import json
import logging
from typing import Literal, override
from datetime import datetime
import config
from base.scraping import scraper, backup_timeout
from base.caching import NullPersistor, Persistor, FilePersistor, SqlitePersistor
from trading.providers.utils import filter_news
from trading.core.securities import Security
from trading.core.news import BaseNewsProvider, News

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

class SeekingAlpha(BaseNewsProvider):
    def __init__(self, storage: Literal['file','db','none']='db'):
        self.news_persistor = FilePersistor(config.caching.file_path/_MODULE/'news') if storage == 'file'\
            else SqlitePersistor(config.caching.db_path, f"{_MODULE}_news") if storage == 'db'\
            else NullPersistor()

    @backup_timeout()
    def _fetch_news(self, unix_from: float, unix_to: float, symbol: str) -> list[News]:
        url = f"https://seekingalpha.com/api/v3/symbols/{symbol}/news?filter[since]={int(unix_from-1000)}&filter[until]={int(unix_to+1000)}&id={symbol}&include=author&isMounting=true&page[size]=50&page[number]="
        i = 1
        ret: list[News] = []
        while True:
            result = scraper.get(url + str(i), cookies={'_px3': 'af68f8fb5b5ccac7de1bcd1e9557a64ca5b54a306698c0183f442b892e57d595:dKFZkKZdn5YMhnVe9/z6Mmeu0Vwm9hHxkSUA12xuWMNzrzBEhvWN72yAkcs/21uiP2FNRzAYmXdyl1IhWgTqig==:1000:XTwygXvwwmUUlYgT7A6afuaPKRfJfP1V/ubqBfweaOzdYj2FzrwVIEpS1yYWXwHLg2Hdu8M4pe1w6fKMCzre5OEMW3L/9/vFIfRzFIQtOXysmQuLcIU33BSX4fmq4wtEMaSHXZ70WNml3AY90zQCEDe/xiIN0Q6JcQNtnFJzkmwCeU3vqCzGsti1r4SuxMkeyr0MVTrVnwWee1wEWlPGdeB57SWQIZ+Dt71z86HzN7I='})
            data = json.loads(result.text)['data']
            for item in data:
                try:
                    item = item['attributes']
                    ret.append(News(datetime.fromisoformat(item['publishOn']).timestamp(), item['title'], ""))
                except:
                    logger.warning(f"Failed to parse news item: {item}.")
                    pass
            if len(data) < 50:
                break
            i += 1
        return ret
    
    #region Overrides
    @override
    def get_news_persistor(self, security: Security) -> Persistor:
        return self.news_persistor
    @override
    def get_news_raw(self, unix_from: float, unix_to: float, security: Security) -> list[News]:
        return filter_news(self._fetch_news(unix_from, unix_to, security.symbol), unix_from, unix_to)
    #endregion