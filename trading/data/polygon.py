from ..utils import httputils, common
from enum import Enum
import math
import time

_API_KEY = "2mpdazNwFmxCMwRVx87Crv4JwoSWwqoe"

class TimeSpan(Enum):
    H1 = '1/hour'

@common.backup_timeout()
def _get_polygon_pricing(
    ticker: str,
    timespan: TimeSpan,
    unix_from: float,
    unix_to: float,
    adjusted: bool = True
) -> dict:
    ticker = ticker.upper()
    unix_from = int(unix_from*1000)
    unix_to = math.ceil(unix_to*1000)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{timespan.value}/{unix_from}/{unix_to}?"
    url += f"adjusted={str(adjusted).lower()}&sort=asc&apiKey={_API_KEY}"
    return httputils.get_as_browser(url)

def get_polygon_pricing(
    ticker: str,
    timestpan: TimeSpan,
    unix_to
):
    pass

