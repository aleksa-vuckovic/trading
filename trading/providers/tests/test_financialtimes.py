from typing import override
import config
from trading.core.pricing import PricingProvider
from trading.core.securities import Security
from trading.core.tests.test_pricing import TestPricingProvider
from trading.providers.financialtimes import FinancialTimes
from trading.providers.nasdaq import NasdaqGS, NasdaqMS, NasdaqCM
from trading.providers.nyse import NYSE, NYSEAmerican, NYSEArca

class TestFinancialtimes(TestPricingProvider):
    @override
    def get_provider(self) -> PricingProvider:
        return FinancialTimes(config.caching.storage)
    @override
    def get_securities(self) -> list[Security]:
        return [
            NasdaqGS.instance.get_security('NVDA'),
            NasdaqMS.instance.get_security('LUNR'),
            NasdaqCM.instance.get_security('RGTI'),
            NYSE.instance.get_security('KO'),
            NYSEAmerican.instance.get_security('IMO'),
            NYSEArca.instance.get_security('SPY')
        ]