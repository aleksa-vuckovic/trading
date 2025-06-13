import asyncio
from typing import Iterable
import logging
from base import dates
from trading.core.pricing import PricingProvider
from trading.core.securities import Security
from trading.models.evaluation.portfolio import Portfolio
from trading.providers.aggregate import AggregateProvider

logger = logging.getLogger(__name__)

class PortfolioManager:
    def __init__(self, portfolio: Portfolio|None = None):
        self.portfolio = portfolio or Portfolio()

    async def run_live(self, securities: Iterable[Security], interval: float):
        securities = set(securities)
        while True:
            action = self.suggest(securities)
            if action:
                self.portfolio.action(action)
            await asyncio.sleep(interval)
            
    def run_historical(
        self,
        securities: Iterable[Security],
        interval: float,
        unix_start: float|None = None,
        unix_end: float|None = None
    ):
        unix_end = unix_end or dates.unix()
        unix_start = unix_start or unix_end - 100*24*3600
        securities = set(securities)

        assert self.portfolio.state.unix_time < unix_start
        unix_time = unix_start
        while unix_time < unix_end:
            tradable = {it for it in securities if it.exchange.calendar.is_worktime(unix_time)}
            action = self.suggest(tradable, unix_time)
            if action:
                logger.info(f"Executing: {action}")
                self.portfolio.action(action)
                logger.info(f"State {self.portfolio.state}")
            unix_time += interval

    #region Abstract
    def suggest(self, securities: set[Security], unix_time: float|None=None) -> Portfolio.Action|None:
        """
        Provide a suggestion based on the current portfolio state, at the current or provided time.
        """
        raise NotImplementedError()
    #endregion
    
