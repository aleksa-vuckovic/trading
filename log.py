#1
from pathlib import Path
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import config
import base.log

def configure_logging(console: bool = False, name: str = "main"):
    date = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    root = Path(config.logging.root)
    output = root / name
    bin = root / "bin"
    if not output.exists(): output.mkdir(parents=True)
    if not bin.exists(): bin.mkdir()
    for file in output.iterdir(): file.rename(bin / file.name)

    #handlers
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(base.log.text_format)
    file_handler_names = ["providers", "yahoo", "models", "http", "others"]
    file_handlers = {}
    for name in file_handler_names:
        handler = RotatingFileHandler(filename=output / f"{date} - {name}.txt", mode="w", maxBytes=1024*1024, backupCount=3, encoding='utf-8')
        handler.setFormatter(base.log.text_format)
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