from .network import Model, extract_tensors
from pathlib import Path
import torch
from torch import optim
from tqdm import tqdm
from ..model1.train import get_training_files, get_validation_files, create_stats
import logging
import config
from ..utils import Batches

logger = logging.getLogger(__name__)
checkpoint_file = Path(__file__).parent / 'checkpoint.pth'
special_checkpoint_file = Path(__file__).parent / 'special_checkpoint.pth'
learning_rate = 10e-6

def run_loop(max_epochs = 100000000):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    training_batches = Batches(get_training_files(), merge=10000//config.batch_size, device = device)
    validation_batches = Batches(get_validation_files(), merge=10000//config.batch_size, device=device)
    logger.info(f"Device: {device}")
    logger.info(f"Loaded {len(training_batches)} training and {len(validation_batches)} validation batches.")

    model = Model().to(device)
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    train_stats = create_stats('train')
    val_stats = create_stats('val')

    if checkpoint_file.exists():
        data = torch.load(checkpoint_file, weights_only=True, map_location=device)
        model.load_state_dict(data['model_state_dict'])
        optimizer.load_state_dict(data['optimizer_state_dict'])
        epoch = data['epoch'] + 1
        history = data['history']
        logger.info(f"Restored state from epoch {epoch-1}")
    else:
        logger.info(f"No state, starting from scratch.")
        epoch = 1
        history = []


    
    while epoch < max_epochs:
        model.train()
        with tqdm(training_batches, desc=f"Epoch {epoch}", leave=True) as bar:
            for batch in bar:
                tensors = extract_tensors(batch)
                input = tensors[:-1]
                expect = tensors[-1]

                optimizer.zero_grad()
                output = model(*input).squeeze()
                loss = train_stats.update(expect, output)
                loss.backward()
                optimizer.step()
                
                bar.set_postfix_str(str(train_stats))
        
        model.eval()
        with torch.no_grad():
            with tqdm(validation_batches, desc = 'Validation...', leave=False) as bar:
                for batch in bar:
                    tensors = extract_tensors(batch)
                    input = tensors[:-1]
                    expect = tensors[-1]

                    output = model(*input).squeeze()
                    val_stats.update(expect, output)
                    
        print(str(val_stats))
        history.append({**train_stats.to_dict(), **val_stats.to_dict(), 'epoch': epoch})
        train_stats.clear()
        val_stats.clear()
        epoch += 1

        savedict = {
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'epoch': epoch,
            'history': history
        }
        torch.save(savedict, checkpoint_file)