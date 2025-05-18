from unittest import TestCase
from typing import Sequence, override
from base import dates
from base.algos import binary_search
from trading.core import Interval
from trading.core.pricing import OHLCV, PricingProvider
from trading.core.securities import Security
from trading.providers.nasdaq import Nasdaq
from trading.models.evaluation.portfolio import Portfolio, Position, SecurityTransaction, Transaction

import unittest.util
unittest.util._MAX_LENGTH = 1000

class MockProvider(PricingProvider):

    def __init__(self, prices: dict[Security, list[tuple[float,float]]]):
        self.prices = prices
    def set_price(self, price: float):
        self.price = price
    def get_pricing(
        self,
        unix_from: float,
        unix_to: float,
        security: Security,
        interval: Interval,
        *,
        interpolate: bool = False,
        max_fill_ratio: float = 1
    ) -> Sequence[OHLCV]:
        if security not in self.prices: return []
        prices = self.prices[security]
        i = binary_search(prices, unix_from, lambda it: it[0], side='GE')
        j = binary_search(prices, unix_to, lambda it: it[0], side='GT')
        return [OHLCV(t,p,p,p,p,p) for t,p in prices[i:j]]
    def get_intervals(self) -> set[Interval]: return set(Interval)
    def get_interval_start(self, interval: Interval) -> float: return 0

s1 = Nasdaq.instance.get_security('NVDA')
s2 = Nasdaq.instance.get_security('GOOG')

def frame(time: float, amount: float) -> Portfolio.EquityFrame:
    return Portfolio.EquityFrame(time, amount)

class TestPortfolio(TestCase):
#130
    """
    10  -10
    70  s1*10*10, -5
    190 -s1*5*15, -5
    250 s2*10*10, 0
    260 s2*10*20, 0
    """
    def test_basic(self):
        dates.set(1000)
        provider = MockProvider({
            s1: [(0, 10),(150,15),(200,20)],
            s2: [(0, 20)]
        })
        portfolio = Portfolio(initial_state=Portfolio.State(0, 0, []), provider=provider)
        
        portfolio.transaction(10, Transaction(10))
        history = portfolio.equity_history(0, 60, Interval.M1)
        expect = [frame(60, -10)]
        self.assertEqual(expect, history)
        ideal_history = portfolio.ideal_equity_history(0, 60, Interval.M1)
        ideal_expect = [frame(60, 0)]
        self.assertEqual(ideal_expect, ideal_history)

        portfolio.transaction(70, SecurityTransaction(s1, 10, 10, 5))
        self.assertEqual(Portfolio.State(70, -115, [Position(s1, 10, 10)]), portfolio.state)
        history = portfolio.equity_history(0, 180, Interval.M1)
        expect = [*expect, frame(120, -15), frame(180, 35)]
        self.assertEqual(expect, history)
        ideal_history = portfolio.ideal_equity_history(0, 180, Interval.M1)
        ideal_expect = [*ideal_expect, frame(120, 0), frame(180, 50)]
        self.assertEqual(ideal_expect, ideal_history)

        portfolio.transaction(190, SecurityTransaction(s1, -5, 15, 5))
        self.assertEqual(Portfolio.State(190, -45, [Position(s1, 5, 10)]), portfolio.state)
        history = portfolio.equity_history(0, 240, Interval.M1)
        expect = [*expect, frame(240, 55)]
        self.assertEqual(expect, history)
        ideal_history = portfolio.ideal_equity_history(0, 240, Interval.M1)
        ideal_expect = [*ideal_expect, frame(240, 75)]
        self.assertEqual(ideal_expect, ideal_history)

        portfolio.transaction(250, SecurityTransaction(s2, 10, 10, 0))
        history = portfolio.equity_history(280, 320, Interval.M1)
        expect = [frame(300, 155)]
        self.assertEqual(expect, history)

        portfolio.transaction(310, SecurityTransaction(s2, -10, 20, 0))
        self.assertEqual(Portfolio.State(310, 55, [Position(s1, 5, 10)]), portfolio.state)

    def test_averaging(self):
        dates.set(1000)
        provider = MockProvider({
            s1: [(0, 10),(150,15),(200,20)],
            s2: [(0, 20)]
        })
        portfolio = Portfolio(initial_state=Portfolio.State(0, 0, []), provider=provider)
        
        portfolio.transaction(10, SecurityTransaction(s1, 10, 60, 0))
        portfolio.transaction(20, SecurityTransaction(s1, 5, 30, 0))
        self.assertEqual(Portfolio.State(20, -750, [Position(s1, 15, 50)]), portfolio.state)
        
    

