import logging
import torch
from pathlib import Path
from torch import optim
from tqdm import tqdm
from ..model1.train import create_stats
from ..utils import Batches, get_batch_files
from .network import Model, extract_tensors

logger = logging.getLogger(__name__)
examples_folder = Path(__file__).parent / 'examples'
checkpoint_file = Path(__file__).parent / 'checkpoint.pth'
special_checkpoint_file = Path(__file__).parent / 'special_checkpoint.pth'
learning_rate = 10e-6

all_files = get_batch_files(examples_folder)
training_files = [it['file'] for it in all_files if it['batch'] % 6]
validation_files = [it['file'] for it in all_files if it['batch']%6 == 0]

def run_loop(max_epochs = 10000):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    training_batches = Batches(training_files, device=device)
    validation_batches = Batches(validation_files, device=device)
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
                logger.info(f"Loaded a batch of shape {batch.shape}")
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
                    
        print(f"Validation: {val_stats}")
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