from trading.data import yahoo, nasdaq, seekingalpha, macrotrends, zacks
from trading.utils import dateutils, httputils, logutils, common
from enum import Enum
import requests
from datetime import datetime, timezone
import re
import time
import pytz
from trading import example
import random
from pathlib import Path
import json
from logging import Logger
from enum import Enum
import shutil
"""cache = Path("./_test_cache")
@common.cached_series(
    cache_root=cache,
    unix_from_arg=0,
    unix_to_arg="unix_to",
    include_args=["type"],
    time_step_fn=lambda args: 10 if args[0] == 'type10' else 30,
    series_field="series",
    timestamp_field="time"
)
def get_series(unix_from: float, *, unix_to: float, type: str):
    return {
        "name": type,
        "series": [{"time": float(it), "data": it} for it in range(int(unix_from), int(unix_to) )]
    }

test1 = get_series(15, unix_to=30, type='type10')
test2 = get_series(15, unix_to=30, type="hm")
print(test1)
print(test2)"""

def decorate(func):
    def wrapper(*args, **kwargs):
        print(f"args {len(args)}")
        print(f"kwargs {len(kwargs)}")
    return wrapper
@decorate
def f(x, y):
    pass

f(1,2)
f(1, y=2)


"""TODO 1. ADD SEPARATE LIVE METHOD IMPL THAT DON'T DIVIDE INTO HUGE CHUNKS!! Applies to all data.
            1.1. Adapt all existing time series methods to the new decorator.
        2. Finish polygon impl, add twelvedata impl
        3. Add fallbacks for pricing
"""


#TODO:  Get outstanding shares to mult with price and get cap data instead of stock price (Get shares outstanding data per year from macrotrends.net)
#       Multiply volume with share price.
#       Skip any examples that would include stock splits (or dilutions?)
#       News titles from seeking alpha .com

# Inputs: market cap normalized by last market cap, volume normalized by last market cap + news + last market cap nonnormalized
# market cap and volume spread out across 2M daily + across 3 days per 30mins
# Output: probability of +10% from last market cap within next 20 30 min slots

# Add special methods for live price fetching, possible fallbacks to other sources
# 

"""
1. Array of price*sharesOutstanding
    --Shares outstanding is the sharesOutstanding at the last input point
    --Normalize by last entry
    --Provide the original last entry again at a later stage

2. Array of volume
    --Normalize by sharesOutstanding

3. 2 recent market snapshots
4. 8 recent stock titles
5. Company summary
"""

"""import base64
def base64_encode(input_string: str) -> str:
    input_bytes = input_string.encode('utf-8')
    base64_bytes = base64.b64encode(input_bytes)
    return base64_bytes.decode('utf-8')

buys = ['Aleksa', 'Hristina', 'Katarina', 'Milica', 'Kaja', 'Anja']
receives = buys[:]
forbidden = [(it,it) for it in buys]
forbidden.extend([('Hristina', 'Katarina'), ('Katarina', 'Hristina')])
import random

random.seed(int(time.time()*1000))
while [it for it in zip(buys, receives) if it in forbidden]:
    random.shuffle(buys)
    random.shuffle(receives)

for buy,rec in zip(buys,receives):
    msg = f"Heey {buy}, you should buy a gift for....................(drumroll)................... {rec.upper()}!"
    print(f"{buy}: {base64_encode(msg)}")"""

