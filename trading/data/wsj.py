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

def _merge_30m_to_1h(data):
    result = []
    dates = [dateutils.unix_to_datetime(it['t']) for it in data]
    lower_bound = 9.5*3600
    upper_bound = 16*3600
    i = 0
    while i < len(data):
        daysecs = (dates[i].hour*60 + dates[i].minute)*60 + dates[i].second
        if daysecs < lower_bound or daysecs >= upper_bound:
            logger.warning(f"Unexpected timestamp {dates[i]}. Skipping entry.")
        elif dates[i].minute == 30:
            if dates[i].hour == 15 or i == len(data)-1:
                result.append(data[i])
            elif dates[i+1].hour == dates[i].hour + 1 and dates[i+1].minute == 0:
                #Merge with next
                data[i]['h'] = max(data[i]['h'],data[i+1]['h'])
                data[i]['l'] = min(data[i]['l'],data[i+1]['l'])
                data[i]['c'] = data[i+1]['c']
                data[i]['v'] += data[i+1]['v']
                result.append(data[i])
                i += 1
            else:
                logger.warning(f"Failed to merge data points for time {dates[i]}. No suitable successor.")
                result.append(data[i])
        else:
            logger.warning(f"Unexpected full hour entry at time {dates[i]}")
        i += 1
    return result
def _fix_timestamps(timestamps: list[float], interval: Interval):
    timestamps = [it//1000 if it else None for it in timestamps]
    if interval == Interval.H1: return timestamps
    if interval == Interval.D1: return fix_daily_timestamps(timestamps)
    else: raise Exception(f"Unexpected interval {interval}")
            

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
    if interval == Interval.H1:
        quotes = _merge_30m_to_1h(quotes)
    result = data['Series'][0]
    del result['DataPoints']
    del result['DesiredDataPoints']
    result['data'] = filter_by_timestamp(quotes, unix_from=unix_from, unix_to=unix_to)
    return result