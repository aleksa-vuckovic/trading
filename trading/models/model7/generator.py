from pathlib import Path
from .. import model5

FOLDER = Path(__file__).parent / 'examples'

class Generator(model5.generator.Generator):
    def run(self):
        self.run_loop(
            folder = FOLDER,
            hour=14
        )