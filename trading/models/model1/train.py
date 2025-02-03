import torch
import logging
from pathlib import Path
from torch import optim
from tqdm import tqdm
from ..utils import StatCollector, StatContainer, Batches, get_batch_files
from .network import Model, extract_tensors

logger = logging.getLogger(__name__)
examples_folder = Path(__file__).parent / 'examples'
checkpoint_file = Path(__file__).parent / 'checkpoint.pth'
special_checkpoint_file = Path(__file__).parent / 'special_checkpoint.pth'
learning_rate = 10e-7

class Accuracy(StatCollector):
    def __init__(self):
        super().__init__('accuracy')
    
    def _calculate(self, expect, output):
        output = output > 0.5
        expect = expect > 0.5
        hits = torch.logical_and(output, expect).sum().item()
        output_n = output.sum().item()
        expect_n = expect.sum().item()
        return hits / output_n if output_n else 0 if expect_n else 1
    
class Precision(StatCollector):
    def __init__(self):
        super().__init__('precision')

    def _calculate(self, expect, output):
        output = output > 0.5
        expect = expect > 0.5
        hits = torch.logical_and(output, expect).sum().item()
        expect_n = expect.sum().item()
        return hits / expect_n if expect_n else 1

class Miss(StatCollector):
    def __init__(self):
        super().__init__('miss')
    
    def _calculate(self, expect, output):
        output = output > 0.2
        misses_n = torch.logical_and(expect < 0, output).sum().item()
        total_n = output.sum().item()
        return misses_n / total_n if total_n else 0
    
class CustomLoss(StatCollector):
    def __init__(self):
        super().__init__('loss')
    
    def _calculate(self, expect, output):
        eps = 1e-5
        loss = -torch.log(1 + eps - torch.abs(output - expect) / (1+torch.abs(expect)))
        return loss.mean()
    
"""
Goals: loss < 0.2, accuracy > 0.7, precision > 0.2, miss < 0.1
"""
def create_stats(name: str) -> StatContainer:
    return StatContainer(CustomLoss(), Accuracy(), Precision(), Miss(), name=name)

all_files = get_batch_files(examples_folder)
training_files = [it['file'] for it in all_files if it['batch'] % 6]
validation_files = [it['file'] for it in all_files if it['batch']%6 == 0]

def run_loop(max_epochs = 100000000):
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

            



                
                
