import os
import torch
import logging
import json
from pathlib import Path
from trading.models.utils import normalize_in_place
from trading.models.model1 import example

examples_bin_folder = Path(__file__).parent / 'examples_bin'
examples_folder = Path(__file__).parent / 'examples'
logger = logging.getLogger(__name__)


def fix_live_series():
    root = Path(__file__).parent / 'trading' / 'data' / 'cache' / 'yahoo'
    for file in sorted(os.listdir(root)):
        #Delete last 150 h1 prices
        path = root / file / 'H1'
        live_path = path / 'live'
        meta_path = path / 'meta'
        if live_path.exists():
            live = json.loads(live_path.read_text())
            meta = json.loads(meta_path.read_text())
            live['data'] = live['data'][:-140]
            if live['data']:
                meta['live']['fetch'] = live['data'][-1]['t']
                live_path.write_text(json.dumps(live))
                meta_path.write_text(json.dumps(meta))
            else:
                print(f'unlink h1 for {file}')
                meta_path.unlink()
                live_path.unlink()
        #Delete last 20 d1 prices
        path = root / file / 'D1'
        live_path = path / 'live'
        meta_path = path / 'meta'
        if live_path.exists():
            live = json.loads(live_path.read_text())
            meta = json.loads(meta_path.read_text())
            live['data'] = live['data'][:-20]
            if live['data']:
                meta['live']['fetch'] = live['data'][-1]['t']
                live_path.write_text(json.dumps(live))
                meta_path.write_text(json.dumps(meta))
            else:
                print(f'unlink d1 for {file}')
                meta_path.unlink()
                live_path.unlink()

def fix_nan_inf():
    files = os.listdir(examples_bin_folder)
    for file in files:
        batch: torch.Tensor = torch.load(examples_bin_folder / file, weights_only=True)
        indices = torch.logical_or(batch.isnan(), batch.isinf())
        indices = torch.sum(indices, dim=1) > 0
        bad_count = indices.sum().item()
        market_cap = batch[indices][:,example.MARKET_CAP_I].mean()
        indices = torch.logical_not(indices)
        good_count = indices.sum().item()
        good_batches = batch[indices]
        target_prices = good_batches[:, example.D1_TARGET_I:] + 1
        last_prices = good_batches[:, example.H1_VOLUMES_I-1].unsqueeze(dim = 1)
        good_batches[:, example.D1_TARGET_I:] = target_prices / last_prices - 1
        normalize_in_place(good_batches, start_index=example.D1_PRICES_I, count=example.D1_PRICES, dim=1)
        normalize_in_place(good_batches, start_index=example.D1_VOLUMES_I, count=example.D1_PRICES, dim=1)
        normalize_in_place(good_batches, start_index=example.H1_PRICES_I, count=example.H1_PRICES, dim=1)
        normalize_in_place(good_batches, start_index=example.H1_VOLUMES_I, count=example.H1_PRICES, dim=1)
        torch.save(good_batches, examples_folder / file)
        logger.info(f'Batch {file}.\t Discard {bad_count}.\t Keep {good_count}.\t Avg mcap: {market_cap}')

def fix_nan_inf_check():
    files = os.listdir(examples_bin_folder)
    total = 0
    for file in files:
        batch: torch.Tensor = torch.load(examples_folder / file, weights_only=True)
        cnt = torch.logical_or(batch.isnan(), batch.isinf()).sum().item()
        if cnt > 0:
            print(f'Bad batch: {file}. Cnt = {cnt}. Examples = {batch.shape[0]}')
            total += 1
        else:
            print(f'Good batch, examples {batch.shape[0]}')
    if total == 0:
        print('All ok')

