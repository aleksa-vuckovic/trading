#1
from pathlib import Path
from sqlalchemy import Engine, create_engine

def sqlite_engine(db_path: Path|str) -> Engine:
    return create_engine(f"sqlite:///{db_path}")

