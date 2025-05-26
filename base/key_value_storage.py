#4
import os
from pathlib import Path
from typing import Any, Iterable, get_origin, override
from sqlalchemy import Engine, delete, exists, select
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, sessionmaker
from base.files import escape_filename, unescape_filename
from base.serialization import Serializer, GenericSerializer

class NotFoundError(Exception):
    pass

class KeyValueStorage:
    def get[T](self, key: str, assert_type: type[T]|None=None) -> T:
        raise NotImplementedError()
    def set(self, key: str, value: Any):
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
        except NotFoundError:
            return None
    
    def get_or_set[T](self, key: str, value: T, assert_type: type[T]|None=None) -> T:
        if not self.has(key):
            self.set(key, value)
        return self.get(key, assert_type)
        
    def compare_and_set(self, key: str, new: Any, old: Any) -> bool:
        if self.has(key) and self.get(key) == old:
            self.set(key, new)
            return True
        return False

class MemoryKVStorage(KeyValueStorage):
    """NOT thread safe."""
    def __init__(self):
        self.data = {}
    
    @override
    def get[T](self, key: str, assert_type: type[T]|None=None) -> T:
        if key not in self.data: raise NotFoundError()
        if assert_type: assert isinstance(self.data[key], get_origin(assert_type) or assert_type)
        return self.data[key]

    @override
    def set(self, key: str, value: Any):
        self.data[key] = value

    @override
    def delete(self, key: str) -> bool:
        if key in self.data:
            del self.data[key]
            return True
        return False

    @override
    def has(self, key: str) -> bool: return key in self.data

    @override
    def keys(self) -> Iterable[str]: return self.data.keys()

class FolderKVStorage(KeyValueStorage):
    """NOT thread safe."""
    def __init__(self, root: Path, serializer: Serializer = GenericSerializer()):
        self.root = root
        self.serializer = serializer
        self.root.mkdir(parents=True, exist_ok=True)
    
    def _path(self, key: str) -> Path: return self.root/escape_filename(key)

    @override
    def get[T](self, key: str, assert_type: type[T]|None=None) -> T:
        path = self._path(key)
        try:
            return self.serializer.deserialize(path.read_text(), assert_type)
        except FileNotFoundError:
            raise NotFoundError()
    
    @override
    def set(self, key: str, value: Any):
        path = self._path(key)
        path.write_text(self.serializer.serialize(value))

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

class FileKVStorage(MemoryKVStorage):
    """NOT thread safe."""
    data: dict[str, Any]
    def __init__(self, path: Path, serializer: Serializer = GenericSerializer()):
        super().__init__()
        self.path = path
        self.serializer = serializer
        if self.path.exists(): self.data = self.serializer.deserialize(self.path.read_text(), dict)
        else: self.path.parent.mkdir(parents=True, exist_ok=True)
    
    def _save(self):
        self.path.write_text(self.serializer.serialize(self.data))
    
    @override
    def set(self, key: str, value: Any):
        super().set(key, value)
        self._save()

    @override
    def delete(self, key: str) -> bool:
        if super().delete(key):
            self._save()
            return True
        return False
    
class SqlKVStorage(KeyValueStorage):
    """Thread and multiprocess safe."""
    def __init__(self, engine: Engine, table_name: str, serializer: Serializer = GenericSerializer()):
        self.engine = engine
        self.serializer = serializer
        self.maker = sessionmaker(bind=engine)
        self.serializable_maker = sessionmaker(bind=engine.execution_options(isolation_level="SERIALIZABLE"))

        Base = declarative_base()
        class Table(Base):
            __tablename__ = table_name
            key: Mapped[str] = mapped_column(primary_key=True)
            value: Mapped[str]
        Base.metadata.create_all(self.engine)
        self.Table = Table
    
    @override
    def get[T](self, key: str, assert_type: type[T]|None = None) -> T:
        with self.maker.begin() as sess:
            result = sess.execute(select(self.Table.value).where(self.Table.key == key)).scalars().all()
        if not result: raise NotFoundError()
        return self.serializer.deserialize(result[0], assert_type)

    @override
    def set(self, key: str, value: Any):
        with self.maker.begin() as sess:
            sess.merge(self.Table(key = key, value = self.serializer.serialize(value)))

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
    
    @override
    def get_or_set[T](self, key: str, value: T, assert_type: type[T] | None = None) -> T:
        with self.serializable_maker.begin() as sess:
            existing = sess.execute(select(self.Table).where(self.Table.key == key)).scalars().one_or_none()
            if not existing: sess.add(self.Table(key = key, value = self.serializer.serialize(value)))
            else: value = self.serializer.deserialize(existing.value, assert_type)
        return value
    
    @override
    def compare_and_set(self, key: str, new: Any, old: Any) -> bool:
        with self.serializable_maker.begin() as sess:
            existing = sess.execute(select(self.Table).where(self.Table.key == key)).scalars().one_or_none()
            if not existing: return False
            if self.serializer.deserialize(existing.value) == old:
                existing.value = self.serializer.serialize(new)
                return True
        return False
    