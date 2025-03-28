from pathlib import Path
from typing import NamedTuple


from trading.core import Interval
from trading.core.work_calendar import TimingConfig
from trading.models.base.model_config import Aggregation, ModelConfig, PriceTarget, Quote, PriceEstimator

class PlanConfig(NamedTuple):
    model_config: ModelConfig
    folders: list[Path]
    checkpoint: Path

"""configs = [
    PlanConfig(
        model_config = ModelConfig(
            estimator = PriceEstimator(Quote.C, Interval.M5, slice(1, 50), Aggregation.MAX, 0.3),
            target = PriceTarget.LINEAR_0_10,
            timing = TimingConfig
        )
    )

]"""