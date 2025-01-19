from trading.data import yahoo, nasdaq, seekingalpha, macrotrends, zacks, polygon
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

logutils.configure_logging()
logging.getLogger("haha").info("hello")



"""TODO 1. Finish polygon impl, add twelvedata impl
        2. Add fallbacks for pricing
"""

