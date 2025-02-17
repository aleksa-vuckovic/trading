import logging
import re
import os
import torch
from pathlib import Path
from trading.models.utils import get_batch_files
from trading.models.model2.generator import D1_DATA, H1_DATA
logger = logging.getLogger(__name__)

def fix_model2_examples(root = Path(__file__).parent / 'trading' / 'models' / 'model2' / 'examples'):
    def fix(tensor):
        close = tensor[:,:,1].clone()
        high = tensor[:,:,3].clone()
        tensor[:,:,1] = high
        tensor[:,:,3] = close
    for file in get_batch_files(root):
        path: Path = file['path']
        data = torch.load(path, weights_only=True)
        fix(data[D1_DATA])
        fix(data[H1_DATA])
        torch.save(data, path)
        print(f"Fixed {path}")