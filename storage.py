from pathlib import Path
from trading.models.abstract import DataConfig
from trading.utils.common import Interval
from trading.utils.dateutils import TimingConfig

class generator:
    data_config = DataConfig({
        Interval.W1: 50,
        Interval.D1: 150,
        Interval.H1: 150,
        Interval.M15: 200,
        Interval.M5: 200
    })
    after_data_config = DataConfig({
        Interval.D1: 5,
        Interval.H1: 2*7,
        Interval.M15: 2*7*4,
        Interval.M5: 2*7*12
    })
    timing = TimingConfig.Builder()\
        .starting(hour = 10, minute = 0)\
        .until(hour = 14, minute=30)\
        .build()\
        .for_interval(Interval.M5)
    folder = Path(__file__).parent/"trading"/"models"/"generators"/"examples_w1_m5"