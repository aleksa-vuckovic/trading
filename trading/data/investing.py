import json
import logging
import math
from pathlib import Path
from ..utils import httputils, common
from ..utils.common import Interval
from .caching import cached_scalar, CACHE_ROOT

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = CACHE_ROOT / _MODULE

def _interval_to_str(interval: Interval) -> str:
  if interval == Interval.H1: return '60'
  if interval == Interval.D1: return 'D'

carrier = "bc26ca13e9d438b569fcb3b5769c64e2/1738843815"

@cached_scalar(
    include_args=[0],
    path_fn=lambda args: _CACHE/common.escape_filename(args[0])/'info'
)
@httputils.backup_timeout()
def _get_info(ticker: str, exchange: str = 'NASDAQ') -> dict:
  url = f"https://tvc4.investing.com/{carrier}/1/1/8/symbols?symbol={exchange.upper()}%20%3A{ticker.upper()}"
  resp = httputils.get_as_browser(url)
  return json.loads(resp.text)

def get_info(ticker: str) -> dict:
  return _get_info(ticker)


_quotes = ['t','o','c','l','h','v']
_indices = { q:i for i,q in enumerate(_quotes) }

def _get_pricing(ticker: str, unix_from: float, unix_to: float, interval: Interval) -> list[list[float]]:
  info = get_info(ticker)
  if info is None or 'ticker' not in info or interval.value not in info['supported_resolutions']:
    logger.error(f"Can't get pricing for {ticker} for resolution {interval}. Info = {info}.")
    return []
  ticker = info['ticker']
  unix_from = int(unix_from)
  unix_to = math.ceil(unix_to)
  url = f"https://tvc4.investing.com/58a16e76cc4c411b480ca39abbdfc764/1738769433/1/1/8/history?symbol={ticker}&resolution={_interval_to_str(interval)}&from={unix_from}&to={unix_to}"
  resp = httputils.get_as_browser(url)
  data = json.loads(resp.text)
  if data['s'] != 'ok':
    logger.error(f"Failed to fetch investing.com pricing. Status = '{data['s']}'. Response: {resp.text}")
    return []
  if not 't' in data: return []
  return [[float(data[q][i]) for q in _quotes] for i in range(len(data['t'])) if data['t'][i] and data['v'][i]]

"""
Investing.com issue: What is the first path segment?
tradingeconomics issue: No volumes
polygon issue: on way to fetch intraday for current day.
"""