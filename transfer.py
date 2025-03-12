from trading.utils import logutils
logutils.configure_logging()
import logging
from pathlib import Path
from trading.core import work_calendar
from trading.core.interval import Interval
from trading.models import model1, model2, model3
from trading.models.evaluator import Evaluator, PriceEstimator, MarketCapSelector, RandomSelector, FirstTradeTimeSelector
from trading.models.utils import PriceTarget

logger = logging.getLogger(__name__)

plan = model2.train.get_plan(hour = 11)

evaluator = Evaluator(model2.generator.Generator(), plan.model)
unix_from = work_calendar.str_to_unix('2025-01-03 00:00:00')
unix_to = work_calendar.str_to_unix('2025-01-18 00:00:00')
selector = MarketCapSelector(top_count=10, select_at=0.5)
estimator = PriceEstimator(interval=Interval.H1, quote='close', min_count=2)
evaluator.backtest(unix_from, unix_to, 11, selector=selector, estimator=estimator)

#model2.generator.Generator().plot_statistics(target=PriceTarget.SIGMOID_0_10)

#Evaluator.show_backtest(file = Path(r"D:\Trading\trading\models\backtests\backtest_Model_hour11_t1739742909.json"))
#train.run_loop()



"""
Show backtest results from memory.

Backtest on january! Try different backtesting strategies.
Keep learning rate higher in the beginning.
Try sigmoid output?
6. Train model7,8,9. Backtest, Evaluate? Make model10,11?
Make model:
    That fills missing prices linearly between known edges
    And that is more strict on the number of available after prices!
        That should also gurantee strictnes on number of prices leading up to the end...
    Both at the same time, as the first only makes sense with the second.
    Also, in that case maybe convolutions should work on 5 step basis? But match the week exactly?
"""