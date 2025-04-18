#1
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path
import config

def configure_logging(console: bool = False, name: str = "main"):
    date = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    root = config.logging.root
    output = root / name
    bin = root / "bin"
    if not output.exists(): output.mkdir(parents=True)
    if not bin.exists(): bin.mkdir()
    for file in output.iterdir(): file.rename(bin / file.name)

    #formatters
    simple_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    timed_simple_formatter = logging.Formatter('%(asctime)s \t- %(name)s - %(levelname)s - %(message)s')

    #handlers
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(simple_formatter)
    file_handler_names = ["providers", "yahoo", "models", "http", "others"]
    file_handlers = {}
    for name in file_handler_names:
        handler = RotatingFileHandler(filename=output / f"{date} - {name}.txt", mode="w", maxBytes=1024*1024, backupCount=3, encoding='utf-8')
        handler.setFormatter(timed_simple_formatter)
        file_handlers[name] = handler

    #loggers
    root = logging.getLogger()
    root.addHandler(file_handlers["others"])
    providers = logging.getLogger("trading.providers")
    providers.propagate = False
    providers.addHandler(file_handlers["providers"])
    yahoo = logging.getLogger("trading.providers.yahoo")
    yahoo.propagate = False
    yahoo.addHandler(file_handlers["yahoo"])
    models = logging.getLogger("trading.models")
    models.propagate = False
    models.addHandler(file_handlers["models"])
    http = logging.getLogger("base.scraping")
    http.propagate = False
    http.addHandler(file_handlers["http"])
    for logger in [root, providers, yahoo, models, http]:
        if console:
            logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)