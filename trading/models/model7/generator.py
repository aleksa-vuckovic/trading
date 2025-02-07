from pathlib import Path
from ..model5 import generator

def run_ordered_loop():
    generator.run_ordered_loop(hour=14, folder=Path(__file__).parent)