from typing import override
import config
from trading.core.pricing import PricingProvider
from trading.core.securities import Security
from trading.core.tests.test_pricing import TestPricingProvider
from trading.providers.nasdaq import Nasdaq
from trading.providers.forex import Forex
from trading.providers.wallstreetjournal import WallStreetJournal

class TestWallStreetJournal(TestPricingProvider):
    @override
    def get_provider(self) -> PricingProvider:
        return WallStreetJournal(config.caching.storage)
    @override
    def get_securities(self) -> list[Security]:
        return [Nasdaq.instance.get_security('NVDA'),Forex.instance.get_security('EURUSD')]
