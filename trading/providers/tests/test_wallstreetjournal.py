from typing import override
import config
from trading.core.pricing import PricingProvider
from trading.core.securities import Security
from trading.core.tests.test_pricing import TestPricingProvider
from trading.providers.nasdaq import NasdaqGS, NasdaqMS, NasdaqCM
from trading.providers.forex import Forex
from trading.providers.nyse import NYSE, NYSEAmerican, NYSEArca
from trading.providers.wallstreetjournal import WallStreetJournal

class TestWallStreetJournal(TestPricingProvider):
    @override
    def get_provider(self) -> PricingProvider:
        return WallStreetJournal(config.storage.location)
    @override
    def get_securities(self) -> list[tuple[Security, float]]:
        return [
            (NasdaqGS.instance.get_security('NVDA'), 0.8),
            (NasdaqMS.instance.get_security('LUNR'), 0.8),
            (NasdaqCM.instance.get_security('RGTI'), 0.7),
            (NYSE.instance.get_security('KO'), 0.8),
            (NYSEAmerican.instance.get_security('IMO'), 0.5),
            (NYSEArca.instance.get_security('SPY'), 0.8),
            (Forex.instance.get_security('EURUSD'), 0.8)
        ]
