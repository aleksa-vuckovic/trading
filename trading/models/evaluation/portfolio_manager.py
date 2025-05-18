import time
from typing import Iterable
from base import dates
from blankprod import PricingProvider
from storage import TimingConfig
from trading.core import Interval
from trading.core.securities import Security
from trading.core.timing_config import execution_spots
from trading.models.evaluation.portfolio import Position, Transaction, Portfolio
from trading.providers.aggregate import AggregateProvider


class PortfolioSuggestion:
    def __init__(self, open: list[Position], close: list[Position]):
        self.open = open
        self.close = close

class PortfolioManager:

    def __init__(self, portfolio: Portfolio|None = None, pricing_provider: PricingProvider = AggregateProvider.instance):
        self.portfolio = portfolio or Portfolio()
        self.pricing_provider = pricing_provider

    def tick(self, time:float|None=None): ...
    def run_live(self, securities: Iterable[Security], interval: float):
        pass

    def run_historical(self, securities: Iterable[Security], interval: float, unix_start: float|None = None, unix_end: float|None = None):
        unix_end = unix_end or dates.unix()
        unix_start = unix_start or unix_end - 100*24*3600
        

    def apply(self, suggestion: PortfolioSuggestion):
        pass
        #for close in suggestion.close:
            #price = self.pricing_provider.get_pricing_at(self.time, close.security)
            #self.portfolio.close(position)

    #region Abstract
    def suggest(self) -> PortfolioSuggestion: ...
    #endregion

    
