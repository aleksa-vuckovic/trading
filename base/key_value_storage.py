#4
import os
from pathlib import Path
from typing import Any, Iterable, override
from sqlalchemy import Engine, delete, exists, select
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, sessionmaker
from base.files import escape_filename, unescape_filename
from base.serialization import Serializer, TypedSerializer

class NotFoundError(Exception):
    pass

class KeyValueStorage:
    def set(self, key: str, value: Any):
        raise NotImplementedError()
    def get[T](self, key: str, assert_type: type[T]|None=None) -> T:
        raise NotImplementedError()
    def delete(self, key: str) -> bool:
        raise NotImplementedError()
    def has(self, key: str) -> bool:
        raise NotImplementedError()
    def keys(self) -> Iterable[str]:
        raise NotImplementedError()
    def try_get[T](self, key: str, assert_type: type[T]|None=None) -> T|None:
        try:
            return self.get(key, assert_type)
        except:
            return None

class MemoryKVStorage(KeyValueStorage):
    def __init__(self):
        self.data = {}
    @override
    def set(self, key: str, value: Any):
        self.data[key] = value
    @override
    def get[T](self, key: str, assert_type: type[T]|None=None) -> T:
        if key not in self.data: raise NotFoundError()
        if assert_type: assert isinstance(self.data[key], assert_type)
        return self.data[key]
    @override
    def delete(self, key: str) -> bool:
        if key in self.data:
            del self.data[key]
            return True
        return False
    @override
    def has(self, key: str) -> bool: return key in self.data
    @override
    def keys(self) -> Iterable[str]: return []

class FolderKVStorage(KeyValueStorage):
    def __init__(self, root: Path, serializer: Serializer = TypedSerializer()):
        self.root = root
        self.serializer = serializer
        self.root.mkdir(parents=True, exist_ok=True)
    def _path(self, key: str) -> Path: return self.root/escape_filename(key)
    @override
    def set(self, key: str, value: Any):
        path = self._path(key)
        path.write_text(self.serializer.serialize(value))
    @override
    def get[T](self, key: str, assert_type: type[T]|None=None) -> T:
        path = self._path(key)
        if path.exists():
            return self.serializer.deserialize(path.read_text(), assert_type)
        raise NotFoundError()
    @override
    def delete(self, key: str) -> bool:
        path = self._path(key)
        if path.exists():
            path.unlink()
            return True
        return False
    @override
    def has(self, key: str) -> bool: return self._path(key).exists()
    @override
    def keys(self) -> Iterable[str]: return [unescape_filename(it) for it in os.listdir(self.root)]
    
class FileKVStorage(KeyValueStorage):
    data: dict[str, Any]
    def __init__(self, path: Path, serializer: Serializer = TypedSerializer()):
        self.path = path
        self.serializer = serializer
        if self.path.exists():
            self.data = self.serializer.deserialize(self.path.read_text(), dict)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.data = {}
    def _save(self):
        self.path.write_text(self.serializer.serialize(self.data))
    @override
    def set(self, key: str, value: Any):
        self.data[key] = value
        self._save()
    @override
    def get[T](self, key: str, assert_type: type[T]|None=None) -> T:
        if key in self.data:
            if assert_type: assert isinstance(self.data[key], assert_type)
            return self.data[key]
        raise NotFoundError()
    @override
    def delete(self, key: str) -> bool:
        if key in self.data:
            del self.data[key]
            self._save()
            return True
        return False
    @override
    def has(self, key: str) -> bool: return key in self.data
    @override
    def keys(self) -> Iterable[str]: return self.data.keys()
    
class SqlKVStorage(KeyValueStorage):
    def __init__(self, engine: Engine, table_name: str, serializer: Serializer = TypedSerializer()):
        self.engine = engine
        self.serializer = serializer
        self.maker = sessionmaker(bind=engine)

        Base = declarative_base()
        class Table(Base):
            __tablename__ = table_name
            key: Mapped[str] = mapped_column(primary_key=True)
            value: Mapped[str]
        Base.metadata.create_all(self.engine)
        self.Table = Table
    @override
    def set(self, key: str, value: Any):
        with self.maker.begin() as sess:
            sess.merge(self.Table(key = key, value = self.serializer.serialize(value)))
    @override
    def get[T](self, key: str, assert_type: type[T]|None = None) -> T:
        with self.maker.begin() as sess:
            result = sess.execute(select(self.Table.value).where(self.Table.key == key)).scalars().one_or_none()
        if result is None: raise NotFoundError()
        return self.serializer.deserialize(result, assert_type)
    @override
    def delete(self, key: str) -> bool:
        with self.maker.begin() as sess:
            return sess.execute(delete(self.Table).where(self.Table.key == key)).rowcount > 0
    @override
    def has(self, key: str) -> bool:
        with self.maker.begin() as sess:
            return sess.execute(select(exists().where(self.Table.key == key))).scalar_one()
    @override
    def keys(self) -> Iterable[str]:
        with self.maker.begin() as sess:
            return sess.execute(select(self.Table.key)).scalars().all()