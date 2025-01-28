from trading.utils import dateutils, httputils, logutils, common
logutils.configure_logging()
from trading.data import yahoo, nasdaq, seekingalpha, macrotrends, zacks, polygon, aggregate
import logging
from tqdm import tqdm
from trading.models.model1 import train
from trading.models.model1 import generator, example, test_generator, fix


train.run_loop()
"""try:
    generator.run_ordered_loop(hour=int(sys.argv[1]))
except:
    logger.error('Generator loop exception.', exc_info=True)
"""