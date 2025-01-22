from trading.data import yahoo, nasdaq, seekingalpha, macrotrends, zacks, polygon, aggregate
from trading.utils import dateutils, httputils, logutils, common
from enum import Enum
import requests
from datetime import datetime, timezone
import re
import time
import pytz
from trading.models.model1 import example
import random
from pathlib import Path
import json
from logging import Logger
from enum import Enum
import shutil
import logging
from tqdm import tqdm
from trading.models.model1.network import Model

logutils.configure_logging()

print('hi')
import torch
from torch.autograd import Function
from transformers import BertTokenizer, BertModel


model = Model()
total_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
print(total_params)


"""TODO 1. Finish polygon impl
        2. Add polygon as fallback for pricing
"""

