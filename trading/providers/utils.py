#1
import logging
from typing import Mapping, Sequence
from trading.core.news import News
from trading.core.pricing import OHLCV
logger = logging.getLogger(__name__)

def arrays_to_ohlcv(data: Mapping[str, Sequence[float|int|None]]) -> list[OHLCV]:
    data = {key[0].lower():data[key] for key in data}
    keys = set(data.keys())
    if not keys: return []
    length = len(next(iter(data.values())))
    if not length: return []
    if not all(len(data[key]) == length for key in keys): raise Exception(f"Unequal series lengths. Data:\n{data}")
    if keys.symmetric_difference('tohlcv'): raise Exception(f"Expecting keys tohlcv but got {keys}.")
    return [OHLCV(*(float(data[key][i] or 0) for key in 'tohlcv')) for i in range(length) if all(data[key][i] is not None for key in 'tohlc')]

def filter_ohlcv(data: Sequence[OHLCV], unix_from: float, unix_to: float) -> list[OHLCV]:
    data = sorted(data, key=lambda it: it.t)
    ret: list[OHLCV] = []
    for it in data:
        if it.t <= unix_from: continue
        if ret and ret[-1].t == it.t: continue
        if it.t > unix_to: break
        if not it.is_valid(): continue
        ret.append(it)
    return ret

def filter_news(data: Sequence[News], unix_from: float, unix_to: float) -> list[News]:
    data = sorted(data, key=lambda x: x.time)
    return [it for it in data if it.time > unix_from and it.time <= unix_to]