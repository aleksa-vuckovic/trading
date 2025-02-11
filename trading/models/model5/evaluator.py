import logging
from typing import Callable
from ...data import nasdaq

logger = logging.getLogger()

def evaluate(
    tickers: list[nasdaq.NasdaqListedEntry],
    unix_time: float|None,
    on_update: Callable[[list[dict]]]|None = None):
    """
    Evaluate model results for all tickers, on unix_time or live (if None).
    On update is invoked with an updated sorted list with each new result.
    """
    