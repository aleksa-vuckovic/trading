from __future__ import annotations
from collections import defaultdict
from functools import cached_property
import math
import time
from bisect import insort, insort_right
from typing import Iterable, Sequence, override
from base.algos import binary_search, binsert, interpolate
from base.caching import KeySeriesStorage, KeyValueStorage, cached_series
from base.key_series_storage import MemoryKSStorage
from base.reflection import transient
from base.tests.common import MemoryKVStorage
from base.types import Cloneable, Equatable, Serializable, json_type
from base.utils import get_or_set
from base import dates
from trading.core import Interval
from trading.core.news import Security
from trading.core.pricing import PricingProvider
from trading.providers.aggregate import AggregateProvider


class Transaction(Equatable, Serializable):
    def __init__(self, fees: float):
        self.fees = fees
    def __repr__(self) -> str:
        return f"Transaction(fees={self.fees})"
class SecurityTransaction(Transaction):
    def __init__(self, security: Security, amount: int, price: float, fees: float):
        super().__init__(fees)
        self.security = security
        self.amount = amount
        self.price = price
    def __repr__(self) -> str:
        return f"Transaction(security={self.security.symbol}, amount={self.amount}, price={self.price}, fees={self.fees})"
    

class Position(Equatable, Cloneable):
    def __init__(self, security: Security, amount: int, price: float):
        self.security = security
        self.amount = amount
        self.price = price # The average price per unit
    def __repr__(self) -> str:
        return f"Position(security={self.security.symbol}, amount={self.amount}, price={self.price})"

class Portfolio(Serializable):
    class State(Equatable):
        def __init__(self, unix_time: float, cash: float, positions: list[Position]):
            self.unix_time = unix_time
            self.cash = cash
            self.positions = positions
        def __repr__(self) -> str:
            return f"State(unix_time={self.unix_time}, cash={self.cash}, positions={self.positions})"
    class Action(Equatable, Serializable):
        def __init__(self, unix_time: float, *, transactions: list[Transaction] = []):
            self.unix_time = unix_time
            self.transactions = transactions

        def merge(self, other: Portfolio.Action) -> Portfolio.Action:
            if self.unix_time != other.unix_time: raise Exception(f"Only concurrent actions can be merged.")
            return Portfolio.Action(
                self.unix_time,
                transactions = self.transactions + other.transactions
            )
        
        def __repr__(self) -> str:
            return f"Action(unix_time={self.unix_time}, transactions={self.transactions})"

    class EquityFrame(Equatable):
        def __init__(self, unix_time: float, equity: float):
            self.unix_time = unix_time
            self.equity = equity
        def __repr__(self) -> str:
            return f"EquityFrame(unix_time={self.unix_time}, equity={self.equity})"
    
    action_history: list[Action] # Primary source of truth
    state_history: list[State] # Calculated based on actions
    ideal_state_history: list[State]
    def __init__(self, *, initial_state: State|None = None, actions: list[Action] = [], provider: PricingProvider = AggregateProvider.instance):
        """The initial state, if any, should precede any actions."""
        self.provider = provider
        self.action_history = actions[:]
        self.state_history = [initial_state or Portfolio.State(0, 0, [])]
        self.ideal_state_history = [initial_state or Portfolio.State(0, 0, [])]
        self.equity_ks_storage = MemoryKSStorage[Portfolio.EquityFrame](lambda it: it.unix_time)
        self.equity_kv_storage = MemoryKVStorage()
        self._update_state_history()

    @property
    def state(self) -> Portfolio.State: return self.state_history[-1]
    @property
    def ideal_state(self) -> Portfolio.State: return self.ideal_state_history[-1]

    def action(self, action: Portfolio.Action) -> Portfolio:
        i = binary_search(self.action_history, action.unix_time, lambda it: it.unix_time, side='GE')
        if i < len(self.action_history) and self.action_history[i].unix_time == action.unix_time:
            self.action_history[i] = self.action_history[i].merge(action)
        else:
            self.action_history.insert(i, action)
        self._update_state_history(action.unix_time)
        return self
    def transaction(self, unix_time: float, transaction: Transaction) -> Portfolio:
        return self.action(Portfolio.Action(unix_time, transactions=[transaction]))
    def fee(self, unix_time: float, amount: float) -> Portfolio:
        return self.transaction(unix_time, Transaction(amount))

    def _update_state_history(self, unix_time: float|None = None):
        if unix_time is None: unix_time = self.state_history[0].unix_time+0.1
        action_index = binary_search(self.action_history, unix_time, lambda it: it.unix_time, side='GE')
        state_index = binary_search(self.state_history, unix_time, lambda it: it.unix_time, side='GE')
        ideal_state_index = binary_search(self.ideal_state_history, unix_time, lambda it: it.unix_time, side='GE')
        if state_index == 0: raise Exception(f"Can't overwrite initial state.")

        actions = self.action_history[action_index:]
        self.state_history[state_index:] = Portfolio._get_states(self.state_history[state_index-1], actions, include_fees=True)
        self.ideal_state_history[ideal_state_index:] = Portfolio._get_states(self.ideal_state_history[ideal_state_index-1], actions, include_fees=False)
        
        #invalidate equity
        for key in self.equity_ks_storage.keys():
            self.equity_ks_storage.delete(key, unix_time-1, dates.unix())

    @staticmethod
    def _get_states(state: State, actions: Iterable[Action], *, include_fees: bool) -> Iterable[State]:
        cash = state.cash
        positions = {it.security: it.clone() for it in state.positions}
        for action in actions:
            if not include_fees and not action.transactions: continue
            if include_fees: cash -= sum(it.fees for it in action.transactions)
            for transaction in action.transactions:
                if not isinstance(transaction, SecurityTransaction): continue
                position = get_or_set(positions, transaction.security, lambda security: Position(security, 0, 0))
                if position.amount * transaction.amount > 0:
                    # Adding to a position, so get the new position price average
                    total_amount = position.amount + transaction.amount
                    position.price = position.amount/total_amount*position.price + transaction.amount/total_amount*transaction.price
                    position.amount = total_amount
                else:
                    if abs(position.amount) < abs(transaction.amount) or position.amount == 0:
                        position.price = transaction.price
                    position.amount += transaction.amount
                cash -= transaction.amount*transaction.price
            
            yield Portfolio.State(action.unix_time, cash, [it.clone() for it in positions.values() if it.amount])
    
    def equity_history(self, unix_from: float, unix_to: float|None = None, interval: Interval = Interval.H1) -> Sequence[EquityFrame]:
        return self._equity_history(unix_from, unix_to or dates.unix(), interval, False)
    def ideal_equity_history(self, unix_from: float, unix_to: float|None = None, interval: Interval = Interval.H1) -> Sequence[EquityFrame]:
        return self._equity_history(unix_from, unix_to or dates.unix(), interval, True)
    def _equity_history_key(self, interval: Interval, ideal: bool) -> str: return f"{interval.name}-{ideal}"
    def _equity_history_ks_storage(self, interval: Interval, ideal: bool) -> KeySeriesStorage[EquityFrame]: return self.equity_ks_storage
    def _equity_history_kv_storage(self, interval: Interval, ideal: bool) -> KeyValueStorage: return self.equity_kv_storage
    @cached_series(
        key=_equity_history_key,
        ks_storage=_equity_history_ks_storage,
        kv_storage=_equity_history_kv_storage
    )
    def _equity_history(self, unix_from: float, unix_to: float, interval: Interval, ideal: bool) -> Sequence[EquityFrame]:
        return list(self._get_equity(self.ideal_state_history if ideal else self.state_history, unix_from, unix_to, interval))

    def _get_equity(self, states: Sequence[State], unix_from: float, unix_to: float, interval: Interval) -> Iterable[EquityFrame]:
        if not states: return []
        unix_from = unix_from - unix_from % interval.time() + interval.time()
        times = [unix_from+i*interval.time() for i in range(math.floor((unix_to-unix_from)/interval.time())+1)]
        states = interpolate([it.unix_time for it in states], states, times, 'stair')
        for unix_time, state in zip(times, states):
            yield Portfolio.EquityFrame(unix_time, state.cash + sum(self.provider.get_pricing_at(unix_time, it.security)*it.amount for it in state.positions))

    @override
    def to_json(self) -> json_type:
        return {'action_history': self.action_history, 'initial_state': self.state_history[0]}
    
    @override
    @classmethod
    def from_json(cls, data: json_type) -> Portfolio:
        assert isinstance(data, dict)
        return Portfolio(initial_state=data['initial_state'], actions=data['action_history'])
    