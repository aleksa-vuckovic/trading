import os
import torch
import logging
import json
from pathlib import Path
from trading.models.utils import get_batch_files
from trading.models import model1
from trading.utils import dateutils

logger = logging.getLogger(__name__)

def fix_timestamps():
    root = Path(__file__).parent / 'trading' / 'data' / 'cache' / 'yahoo'
    for folder in os.listdir(root):
        d1_path = root / folder / 'D1'
        h1_path = root / folder / 'H1'
        #Move h1 prices an hour forward
        for file in os.listdir(h1_path) if h1_path.exists() else []:
            path = h1_path / file
            if file != 'meta':
                data = json.loads(path.read_text())
                for entry in data['data']:
                    date = dateutils.unix_to_datetime(entry['t'])
                    if date.hour == 15 and date.minute == 30: entry['t'] += 1800
                    else: entry['t'] += 3600
                    if date.hour < 9 or date.hour > 16 or entry['t']%1800: logger.warning(f"Found bad 1h timestamp for {folder} - {entry['t']}")
                #path.write_text(json.dumps(data))
            else:
                data = json.loads(path.read_text())
                fetch = data['live']['fetch'] + 3600
                if fetch%1:
                    logger.info("Nonfake fetch")
                    data['live']['fetch'] = fetch
                else: data['live']['fetch'] = fetch + 1
                #path.write_text(json.dumps(data))
        for file in os.listdir(d1_path) if d1_path.exists() else []:
            path = d1_path / file
            if file != 'meta':
                data = json.loads(path.read_text())
                for entry in data['data']:
                    date = dateutils.unix_to_datetime(entry['t'])
                    if date.hour == 9 and date.hour == 30: entry['t'] += 6.5*3600
                    elif date.hour == 16 and date.minute == 0: entry['t'] += 0
                    else: logger.warning(f"Found bad 1d timestamp for {folder} - {entry['t']}")
                #path.write_text(json.dumps(data))
            else:
                data = json.loads(path.read_text())
                fetch = data['live']['fetch'] + 13*1800
                if fetch%1:
                    logger.info('Nonfake fetch')
                    data['live']['fetch'] = fetch
                else: data['live']['fetch'] = fetch + 1
                #path.write_text(json.dumps(data))
        logger.info(f"Successfully finished: {folder}")
