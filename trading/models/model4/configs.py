from pathlib import Path
from typing import NamedTuple


from trading.core import Interval
from trading.core.timing_config import TimingConfig
from trading.models.base.model_config import Aggregation, BaseModelConfig, PriceTarget, BarValues, PriceEstimator



"""configs = [
    PlanConfig(
        model_config = ModelConfig(
            estimator = PriceEstimator(Quote.C, Interval.M5, slice(1, 50), Aggregation.MAX, 0.3),
            target = PriceTarget.LINEAR_0_10,
            timing = TimingConfig
        )
    )

]"""