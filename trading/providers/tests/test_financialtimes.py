from typing import override
import config
from trading.core.pricing import PricingProvider
from trading.core.securities import Security
from trading.core.tests.test_pricing import TestPricingProviderRecent
from trading.providers.financialtimes import FinancialTimes
from trading.providers.nasdaq import Nasdaq

class TestFinancialtimes(TestPricingProviderRecent):
    @override
    def get_provider(self) -> PricingProvider:
        return FinancialTimes(config.caching.storage)
    @override
    def get_securities(self) -> list[Security]:
        return [Nasdaq.instance.get_security('NVDA')]