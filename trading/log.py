#1
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path

def configure_logging(testing: bool = False, console: bool = False, folder: Path = Path("./logs/main")):
    date = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    logroot = folder/"test" if testing else folder/"prod"
    logbin = folder/"bin"
    if not logroot.exists():
        logroot.mkdir(parents=True)
    if not logbin.exists():
        logbin.mkdir()
    for file in logroot.iterdir():
        file.unlink() if testing else file.rename(logbin / file.name)

    #formatters
    simple_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    timed_simple_formatter = logging.Formatter('%(asctime)s \t- %(name)s - %(levelname)s - %(message)s')

    #handlers
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(simple_formatter)
    file_handler_names = ["securities", "yahoo", "models", "http", "others"]
    file_handlers = {}
    for name in file_handler_names:
        handler = RotatingFileHandler(filename=logroot / f"{date} - {name}.txt", mode="w", maxBytes=1024*1024, backupCount=3)
        handler.setFormatter(timed_simple_formatter)
        file_handlers[name] = handler

    #loggers
    root = logging.getLogger()
    root.addHandler(file_handlers["others"])
    securities = logging.getLogger("trading.securities")
    securities.propagate = False
    securities.addHandler(file_handlers["securities"])
    yahoo = logging.getLogger("trading.securities.yahoo")
    yahoo.propagate = False
    yahoo.addHandler(file_handlers["yahoo"])
    nasdaq = logging.getLogger("trading.securities.nasdaq")
    nasdaq.propagate = False
    nasdaq.addHandler(logging.NullHandler())
    models = logging.getLogger("trading.models")
    models.propagate = False
    models.addHandler(file_handlers["models"])
    http = logging.getLogger("trading.utils.httputils")
    http.propagate = False
    http.addHandler(file_handlers["http"])
    for logger in [root, securities, yahoo, models, http]:
        if console:
            logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)