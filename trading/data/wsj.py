https://api.wsj.net/api/michelangelo/timeseries/history?json={"Step":"PT1H","TimeFrame":"D5","IncludeMockTick":true,"FilterNullSlots":false,"FilterClosedPoints":true,"IncludeClosedSlots":false,"IncludeOfficialClose":true,"InjectOpen":false,"ShowPreMarket":false,"ShowAfterHours":false,"UseExtendedTimeFrame":true,"WantPriorClose":false,"IncludeCurrentQuotes":false,"ResetTodaysAfterHoursPercentChange":false,"Series":[{"Key":"STOCK/US/XNAS/NVDA","Dialect":"Charting","Kind":"Ticker","SeriesId":"s1","DataTypes":["Open","High","Low","Last"],"Indicators":[{"Parameters":[],"Kind":"Volume","SeriesId":"i3"}]}]}&ckey=57494d5ed7


_TOKEN_KEY='Dylan2010.Entitlementtoken'
_TOKEN_VALUE='57494d5ed7ad44af85bc59a51dd87c90'
_CKEY='57494d5ed7'

"""
Since wsj provides round hour values, use 15 min and combine into 1hour!!
"""
def _get_pricing(ticker: str, unix_from: float, unix_to: float, exchange: str = 'XNAS'):
    pass