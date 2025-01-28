from trading.utils import dateutils, httputils, logutils, common
logutils.configure_logging()
from trading.data import yahoo, nasdaq, seekingalpha, macrotrends, zacks, polygon, aggregate
import logging
from tqdm import tqdm
from trading.models.model1 import train
from trading.models.model1 import generator, example, test_generator, fix
#print(example.D7_TARGET_I)
#train.run_loop()
fix.fix_nan_inf()
"""try:
    generator.run_ordered_loop(hour=int(sys.argv[1]))
except:
    logger.error('Generator loop exception.', exc_info=True)
"""
"""
TODO:
    DELETE ALL EXAMPLES CONTAINING INF VOLUME / 0 MARKET CAP
        Rearrange the rest into 1000 pack batches.
        Renormalize volumes for the rest.
    DO NOT USE SHARES IN EXAMPLE GENERATION, NORMALIZE VOLUME BY LAST VOLUME

"""