from pathlib import Path
from typing import Any, Literal, override
from unittest import TestCase
import shutil
from pymongo.database import Database
from sqlalchemy import Engine
from base import dates, db
from base import mongo
from base.algos import random_b32
from base.caching import KeySeriesStorage, KeyValueStorage
from base.key_series_storage import FolderKSStorage, MemoryKSStorage, MongoKSStorage, SqlKSStorage
from base.key_value_storage import FileKVStorage, FolderKVStorage, MemoryKVStorage, MongoKVStorage, SqlKVStorage
from base.types import Equatable, Serializable
import config
import injection

class A(Equatable, Serializable):
    def __init__(self, t: float, d: Any = None):
        self.t = float(t)
        self.d = d
    def __repr__(self) -> str: return f"A(t={self.t},d={self.d})"

type storage_type = Literal['mem', 'folder', 'file', 'sqlite', 'mongo']
kv_types = ['mem', 'folder', 'file', 'sqlite', 'mongo']
ks_types = ['mem', 'folder', 'sqlite', 'mongo']

root = Path(config.storage.local_root_path_tmp)

class TestPersistence(TestCase):
    _sqlite_engine: Engine|None
    _mongodb: Database|None
    @property
    def sqlite_engine(self) -> Engine:
        if not self._sqlite_engine: self._sqlite_engine = injection.local_db_tmp
        return self._sqlite_engine
    @property
    def mongodb(self) -> Database:
        if self._mongodb is None: self._mongodb = injection.mongo_db_tmp
        return self._mongodb
    def get_kv_storage(self, storage_type: storage_type) -> KeyValueStorage:
        if storage_type == 'mem': return MemoryKVStorage()
        if storage_type == 'folder': return FolderKVStorage(root/random_b32())
        if storage_type == 'file': return FileKVStorage(root/random_b32())
        if storage_type == 'sqlite': return SqlKVStorage(self.sqlite_engine, random_b32())
        if storage_type == 'mongo': return MongoKVStorage(self.mongodb[random_b32()])
        raise Exception(f"Unsupported kv storage type {storage_type}.")
    def get_ks_storage(self, type: storage_type) -> KeySeriesStorage:
        if type == 'mem': return MemoryKSStorage[A](lambda it: it.t)
        if type == 'folder': return FolderKSStorage[A](root/random_b32(), lambda it: it.t)
        if type == 'sqlite': return SqlKSStorage[A](self.sqlite_engine, 'test', lambda it: it.t)
        if type == 'mongo': return MongoKSStorage[A](self.mongodb[random_b32()], lambda it: it.t)
        raise Exception(f"Unsupported ks storage type {type}.")
    @override
    def setUp(self):
        super().setUp()
        self._sqlite_engine = None
        self._mongodb = None
        dates.set(None)
    @override
    def tearDown(self):
        super().tearDown()
        if self._sqlite_engine:
            db.drop_all(self._sqlite_engine)
            self._sqlite_engine = None
        if self._mongodb is not None:
            mongo.clear(self._mongodb)
            self._mongodb = None
        if root.exists():
            shutil.rmtree(root)
        dates.set(None)
