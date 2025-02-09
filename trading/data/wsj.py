import json
import logging
from ..utils.common import Interval
from ..utils import httputils, dateutils
from .utils import combine_series, fix_daily_timestamps, filter_by_timestamp

logger = logging.getLogger(__name__)
_TOKEN_KEY='Dylan2010.Entitlementtoken'
_TOKEN_VALUE='57494d5ed7ad44af85bc59a51dd87c90'
_CKEY='57494d5ed7'



"""
Since wsj provides round hour values, use 15 min and combine into 1hour!!
"""
def _get_pricing_raw(key: str, step: str, time_frame: str):
    url = "https://api.wsj.net/api/michelangelo/timeseries/history"
    request = {
        "Step": step,
        "TimeFrame": time_frame,
        "IncludeMockTick":True,
        "FilterNullSlots":False,
        "FilterClosedPoints":True,
        "IncludeClosedSlots":False,
        "IncludeOfficialClose":True,
        "InjectOpen":False,
        "ShowPreMarket":False,
        "ShowAfterHours":False,
        "UseExtendedTimeFrame":True,
        "WantPriorClose":False,
        "IncludeCurrentQuotes":False,
        "ResetTodaysAfterHoursPercentChange":False,
        "Series": [
            {
                "Key":key,
                "Dialect":"Charting",
                "Kind":"Ticker",
                "SeriesId":"s1",
                "DataTypes": ["Open","High","Low","Last"],
                "Indicators":[
                    {"Parameters":[],"Kind":"Volume","SeriesId":"i2"}
                ]
            }
        ]
    }
    resp = httputils.get_as_browser(url, params={'json': json.dumps(request), 'ckey': _CKEY}, headers={_TOKEN_KEY: _TOKEN_VALUE})
    return json.loads(resp.text)

def _merge_data_1h(data):
    result = []
    dates = [dateutils.unix_to_datetime(it['t']) for it in data]
    lower_bound = 10*3600
    upper_bound = 16*3600
    i = 0
    while i < len(data):
        daysecs = (dates[i].hour*60 + dates[i].minute)*60 + dates[i].second
        if daysecs < lower_bound or daysecs > upper_bound:
            logger.warning(f"Unexpected timestamp {dates[i]}. Skipping entry.")
        elif dates[i].minute == 0:
            if dates[i].hour == 16 or i == len(data)-1:
                result.append(data[i])
            elif dates[i+1].hour == dates[i].hour and dates[i+1].minute == 30:
                #Merge with next
                data[i]['h'] = max(data[i]['h'],data[i+1]['h'])
                data[i]['l'] = min(data[i]['l'],data[i+1]['l'])
                data[i]['c'] = data[i+1]['c']
                data[i]['v'] += data[i+1]['v']
                data[i]['t'] = data[i+1]['t']
                result.append(data[i])
                i += 1
            else:
                logger.warning(f"Failed to merge data points for time {dates[i]}. No suitable successor.")
                data[i]['t'] += 1800
                result.append(data[i])
        else:
            logger.warning(f"Unexpected non full-hour entry at time {dates[i]}. Skipping.")
        i += 1
    return result 

def _fix_timestamps(timestamps: list[float|int|None], interval: Interval) -> list[float|None]:
    timestamps = [it//1000 if it else None for it in timestamps]
    if interval == Interval.D1: return fix_daily_timestamps(timestamps)
    if interval == Interval.H1:
        result = []
        for it in timestamps:
            if not it:
                result.append(None)
            elif it%1800:
                logger.warning(f"Unexpected timestamp {it}. Expecting clean 30min intervals. Skipping.")
                result.append(None)
            else:
                result.append(it+1800)
        return result
    else: raise Exception(f"Unknown interval {Interval}")

def _get_pricing(symbol: str, unix_from: float, unix_to: float, interval: Interval) -> dict:
    if interval == Interval.H1: step = 'PT30M'
    elif interval == Interval.D1: step = 'P1D'
    else: raise ValueError(f"Unknown interval {interval}")
    data = _get_pricing_raw(f"STOCK/US/XNAS/{symbol.upper()}", step, 'D5')
    def extract_data_points(series: dict) -> dict:
        return {key: [it[index] for it in series['DataPoints']] for index,key in enumerate(series['DesiredDataPoints'])}
    quotes = {'Timestamp': _fix_timestamps(data['TimeInfo']['Ticks'], interval)}
    for series in data['Series']:
        quotes = {**quotes, **extract_data_points(series)}
    quotes['Close'] = quotes['Last']
    del quotes['Last']
    quotes = combine_series(quotes)
    if interval == Interval.H1: quotes = _merge_data_1h(quotes)
    result = data['Series'][0]
    del result['DataPoints']
    del result['DesiredDataPoints']
    result['data'] = filter_by_timestamp(quotes, unix_from=unix_from, unix_to=unix_to)
    return result