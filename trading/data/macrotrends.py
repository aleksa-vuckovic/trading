
import re
import logging
from bs4 import BeautifulSoup
from ..utils import httputils
from ..utils.dateutils import XNAS
from .caching import cached_scalar, CACHE_ROOT
from . import nasdaq


logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE = CACHE_ROOT / _MODULE


@cached_scalar(
    include_args=[0],
    cache_root=_CACHE
)
@httputils.backup_timeout()
def _get_shares_outstanding(ticker: str, short_name: str) -> dict[str, str]:
    url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{short_name}/shares-outstanding"
    resp = httputils.get_as_browser(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", class_="historical_data_table")
    if not tables or len(tables) == 0:
        raise Exception(f"Failed to locate 'historical_data_table' in macrotrends page.")
    result = {}
    for table in tables:
        if 'shares outstanding' not in table.find("thead").find("th").get_text().lower():
            continue
        for row in table.find("tbody").find_all("tr"):
            cells = row.find_all("td")
            try:
                values = [cell.get_text(strip=True) for cell in cells]
                result[values[0]] = values[1]
            except:
                logger.error("Failed to load cells for shares outstanding from macrotrends.", exc_info=True)
    res = []
    for key,value in result.items():
        try:
            unix_time = XNAS.str_to_unix(key, format = '%Y' if len(key) == 4 else '%Y-%m-%d')
            shares = int(value.replace(",", ""))
            res.append({'unix_time': unix_time, 'shares': shares*1000000})
        except:
            logger.error("Failed to parse shares outstanding row from macrotrends", exc_info=True)
    return sorted(res, key = lambda x: x['unix_time'])

def get_shares_outstanding(ticker: nasdaq.NasdaqListedEntry) -> list[dict]:
    """
    Returns shares outstanding fully, as provided by macrotrends.
    """
    return _get_shares_outstanding(ticker.symbol.upper(), re.sub(r'\s+', '-', ticker.short_name().strip()).replace(".", "").lower())

def get_shares_outstanding_at(ticker: nasdaq.NasdaqListedEntry, unix_time: int) -> float:
    data = get_shares_outstanding(ticker)
    i = 0
    while i < len(data)-1 and data[i]['unix_time'] < unix_time:
        i += 1
    return float(data[i]['shares'])