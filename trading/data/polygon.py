import math
import json
import logging
from ..utils import httputils, common
from ..utils.dateutils import XNAS
from ..utils.common import Interval
from .caching import cached_series, CACHE_ROOT

logger = logging.getLogger(__name__)
_API_KEY = "2mpdazNwFmxCMwRVx87Crv4JwoSWwqoe"
#_API_KEY = "t_EpcFm2mmah_opokpJm4F7bUqmiSZNN"
_MODULE = __name__.split(".")[-1]
_CACHE = CACHE_ROOT / _MODULE

def _interval_to_str(interval: Interval) -> str:
    if interval == Interval.H1: return '1/hour'
    if interval == Interval.D1: return '1/day'
    raise Exception(f'Unknown interval {Interval}')

@cached_series(
    unix_from_arg=1,
    unix_to_arg=2,
    include_args=[0,3],
    cache_root=_CACHE,
    time_step_fn=lambda args: 10000000 if args[1] == common.Interval.H1 else 50000000,
    series_field="results",
    timestamp_field="t",
    live_delay_fn=0,
    live_refresh_fn=lambda args,last,now: XNAS.get_next_timestamp(last, args[1]) > now,
    return_series_only=True
)
@httputils.backup_timeout()
def _get_polygon_pricing(
    ticker: str,
    unix_from: float,
    unix_to: float,
    timespan: Interval,
    adjusted: bool = True
) -> dict:
    ticker = ticker.upper()
    unix_from = int(unix_from*1000)
    unix_to = math.ceil(unix_to*1000)
    logger.info(f"Fetching from {unix_from} to {unix_to}")
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{_interval_to_str(timespan)}/{unix_from}/{unix_to}?"
    url += f"adjusted={str(adjusted).lower()}&sort=asc&apiKey={_API_KEY}"
    resp = httputils.get_as_browser(url)
    data = json.loads(resp.text)
    def adjust_timestamps(data):
        for i in range(len(data['results'])):
            data['results'][i]['t'] /= 1000.0
    adjust_timestamps(data)
    while 'next_url' in data and data['next_url']:
        break
        logger.info(f"Next page. Results count = {data['resultsCount'] if 'resultsCount' in data else None}")
        logger.info(f"RESULTS[-5:] (total={len(data['results'])}) = \n{data['results'][-3:]}")
        resp = httputils.get_as_browser(f"{data['next_url']}&adjusted={str(adjusted).lower()}&sort=asc&apiKey={_API_KEY}")
        new_data = json.loads(resp.text)
        adjust_timestamps(new_data)
        data['results'].extend(new_data['results'])
        data['next_url'] = new_data['next_url'] if 'next_url' in new_data else None
    return data

def get_polygon_pricing(
    ticker: str,
    unix_from: float,
    unix_to: float,
    timespan: Interval
):
    return _get_polygon_pricing(ticker.upper(), unix_from, unix_to, timespan, True)

