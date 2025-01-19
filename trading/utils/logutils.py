import logging
from . import dateutils
from pathlib import Path

def configure_logging():
    date = str(dateutils.now(tz = dateutils.CET).strftime("%Y-%m-%d %H-%M-%S"))
    logroot = Path("./logs")
    logbin = Path("./logsbin")
    if not logbin.exists():
        logbin.mkdir()
    if not logroot.exists():
        logroot.mkdir()
    else:
        for file in logroot.iterdir():
            file.rename(logbin / file.name)

    #formatters
    simple_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    timed_simple_formatter = logging.Formatter('%(asctime)s \t- %(name)s - %(levelname)s - %(message)s')

    #handlers
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(simple_formatter)
    file_handler_names = ["data", "yahoo", "models", "others"]
    file_handlers = {}
    for name in file_handler_names:
        handler = logging.FileHandler(filename=logroot / f"{date} - {name}", mode="w")
        handler.setFormatter(simple_formatter)
        file_handlers[name] = handler

    #loggers
    root = logging.getLogger()
    root.addHandler(file_handlers["others"])
    data = logging.getLogger("trading.data")
    data.propagate = False
    data.addHandler(file_handlers["data"])
    yahoo = logging.getLogger("trading.data.yahoo")
    yahoo.propagate = False
    yahoo.addHandler(file_handlers["yahoo"])
    models = logging.getLogger("trading.models")
    models.propagate = False
    models.addHandler(file_handlers["models"])
    for logger in [root, data, yahoo, models]:
        logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)