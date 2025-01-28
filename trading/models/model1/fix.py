from . import example
from pathlib import Path
import os
import torch
import logging
from ...utils import common

examples_bin_folder = Path(__file__).parent / 'examples_bin'
examples_folder = Path(__file__).parent / 'examples'
logger = logging.getLogger(__name__)

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
        common.normalize_in_place(good_batches, start_index=example.D1_PRICES_I, count=example.D1_PRICES, dim=1)
        common.normalize_in_place(good_batches, start_index=example.D1_VOLUMES_I, count=example.D1_PRICES, dim=1)
        common.normalize_in_place(good_batches, start_index=example.H1_PRICES_I, count=example.H1_PRICES, dim=1)
        common.normalize_in_place(good_batches, start_index=example.H1_VOLUMES_I, count=example.H1_PRICES, dim=1)
        torch.save(good_batches, examples_folder / file)
        logger.info(f'Batch {file}.\t Discard {bad_count}.\t Keep {good_count}.\t Avg mcap: {market_cap}')

