from pathlib import Path
from trading.models.base.model_config import PricingDataConfig
from trading.core.interval import Interval
from trading.core.work_calendar import TimingConfig
from trading.providers import Nasdaq

class generator:
    data_config = PricingDataConfig({
        Interval.W1: 50,
        Interval.D1: 150,
        Interval.H1: 150,
        Interval.M15: 200,
        Interval.M5: 200
    })
    after_data_config = PricingDataConfig({
        Interval.D1: 5,
        Interval.H1: 2*7,
        Interval.M15: 2*7*4,
        Interval.M5: 2*7*12
    })
    timing = TimingConfig.Builder()\
        .starting(hour = 10, minute = 0)\
        .until(hour = 14, minute=30)\
        .build()
    folder = Path(__file__).parent/"trading"/"models"/"generators"/"examples_w1_m5"

class generator2:
    data_config = PricingDataConfig({
        Interval.W1: 50,
        Interval.D1: 150,
        Interval.H1: 150,
        Interval.M15: 200
    })
    after_data_config = PricingDataConfig({
        Interval.D1: 5,
        Interval.H1: 2*7,
        Interval.M15: 2*7*4,
        Interval.M5: 2*7*12
    })
    timing = TimingConfig.Builder()\
        .starting(hour = 10, minute = 0)\
        .until(hour = 15, minute=30)\
        .build()
    folder = Path(__file__).parent/"trading"/"models"/"generators"/"examples_w1_m15"