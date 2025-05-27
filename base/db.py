#1
from pathlib import Path
from sqlalchemy import Engine, MetaData, create_engine

def sqlite_engine(db_path: Path|str) -> Engine:
    return create_engine(f"sqlite:///{db_path}")

def drop_all(engine: Engine):
    metadata = MetaData()
    metadata.reflect(engine)
    metadata.drop_all(engine)
