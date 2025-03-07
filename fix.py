import logging
import re
import os
import torch
import json
import config
from datetime import timedelta
from pathlib import Path
from trading.utils.dateutils import TimingConfig
from trading.utils import dateutils
from trading.data import aggregate
from trading.utils.common import Interval
from trading.utils.dateutils import XNAS
from trading.data.caching import FilePersistor, SqlitePersistor
from tqdm import tqdm
import shutil


logger = logging.getLogger(__name__)

def migrate():
    def migrate_cache(root: Path, db: Path, table: str):
        p1 = FilePersistor(root)
        p2 = SqlitePersistor(db, table)
        p1.migrate(p2)

    db = Path("./cache.db")
    cache = Path("./trading/data/cache")

    migrate_cache(cache/'financialtimes'/'info', db, 'financialtimes_info')
    migrate_cache(cache/'financialtimes'/'pricing', db, 'financialtimes_pricing')
    migrate_cache(cache/'globenewswire'/'news', db, 'globenewswire_news')
    migrate_cache(cache/'seekingalpha'/'news', db, 'seekingalpha_news')
    migrate_cache(cache/'wallstreetjournal'/'pricing', db, 'wallstreetjournal_pricing')
    migrate_cache(cache/'yahoo'/'info', db, 'yahoo_info')
    migrate_cache(cache/'yahoo'/'pricing', db, 'yahoo_pricing')


def nest(folder: Path, into: str):
    temp = folder.parent / (folder.parts[-1]+'2')
    folder.rename(temp)
    folder.mkdir(parents=True)
    temp.rename(folder/into)

def fix_pricing(root: Path):
    (root/'info').mkdir(parents=True, exist_ok=True)
    (root/'pricing').mkdir(parents=True, exist_ok=True)
    for ticker in os.listdir(root):
        if ticker == 'info' or ticker == 'pricing': continue
        path = root/ticker/'info'
        if path.exists(): path.rename(root/'info'/ticker)
        (root/ticker).rename(root/'pricing'/ticker)        

def fix_cache():
    cache = Path('./trading')/'data'/'cache'
    fix_pricing(cache/'financialtimes')
    fix_pricing(cache/'yahoo')
    (cache/'wallstreetjournal').mkdir(parents=True)
    (cache/'wsj').rename(cache/'wallstreetjournal'/'pricing')
    shutil.rmtree(cache/'polygon', ignore_errors=True)
    shutil.rmtree(cache/'macrotrends', ignore_errors=True)
    nest(cache/'seekingalpha', 'news')
    nest(cache/'zacks', 'summary')
    nest(cache/'globenewswire', 'news')


def clear_cache(module: str):
    root = Path(__file__).parent / 'trading' / 'data' / 'cache' / module
    for ticker in os.listdir(root):
        for interval in Interval:
            folder = root / ticker / interval.name
            if folder.exists():
                shutil.rmtree(folder)
                logger.info(f"Removed {folder}")
                
def fix_timestamps():
    root = Path(__file__).parent / 'trading' / 'data' / 'cache' / 'yahoo'
    for ticker in tqdm(os.listdir(root), desc='Fixing...'):
        ticker_folder = root / ticker
        for interval in Interval:
            folder = ticker_folder / interval.name 
            if not folder.exists(): continue
            for file in os.listdir(folder):
                if file == 'meta': continue
                file = folder / file
                info = json.loads(file.read_text())
                data = info['data']
                result = []
                for entry in data:
                    timestamp = entry['t']
                    date = XNAS.unix_to_datetime(timestamp)
                    if XNAS.is_timestamp(timestamp, interval):
                        result.append(entry)
                    elif interval == Interval.L1:
                        fixed_date = XNAS.get_next_timestamp(date.replace(day=1), interval)
                        entry['t'] = fixed_date.timestamp()
                        result.append(entry)
                        logger.info(f"Fixed {interval} from {date} to {fixed_date}")
                    elif interval == Interval.W1:
                        fixed_date = XNAS.get_next_timestamp(date-timedelta(days=date.weekday()+1), interval)
                        entry['t'] = fixed_date.timestamp()
                        result.append(entry)
                        logger.info(f"Fixed {interval} from {date} to {fixed_date}")
                    elif interval == Interval.D1:
                        if not XNAS.is_workday(date):
                            logger.info(f"Deleting {interval} {date}")
                        else:
                            fixed_date = XNAS.set_close(date)
                            entry['t'] = fixed_date.timestamp()
                            result.append(entry)
                            logger.info(f"Fixed {interval} from {date} to {fixed_date}")
                    elif interval == Interval.H1:
                        if timestamp-1800 == XNAS.set_close(timestamp):
                            entry['t'] = timestamp-1800
                            result.append(entry)
                            logger.info(f"Fixed {interval} from {date} to {XNAS.unix_to_datetime(timestamp-1800)}")
                        else:
                            logger.info(f"Deleting {interval} {date}")
                    else:
                        logger.info(f"Deleting {interval} {date}")
                info['data'] = result
                file.write_text(json.dumps(info))

                