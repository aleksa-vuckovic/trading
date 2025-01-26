from trading.utils import dateutils, httputils, logutils, common
logutils.configure_logging()
from trading.data import yahoo, nasdaq, seekingalpha, macrotrends, zacks, polygon, aggregate
from logging import Logger
import logging
from tqdm import tqdm
from trading.models.model1.network import Model
import torchinfo
import torch
from trading.models.model1 import generator, example, test_generator
#model = Model()
#total_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
#print(total_params)
import time
import sys
"""
logger = logging.getLogger(__name__)

try:
    generator.run_ordered_loop(hour=int(sys.argv[1]))
except:
    logger.error('Generator loop exception.', exc_info=True)"""

"""TODO 1. Finish polygon impl
        2. Add polygon as fallback for pricing
"""

