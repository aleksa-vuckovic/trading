from __future__ import annotations
from parameterized import parameterized
from base.tests.common import TestPersistence, storage_type, kv_types

class TestKVStorage(TestPersistence):
    
    @parameterized.expand(kv_types, skip_on_empty=True)
    def test_kv_storage_simple(self, storage_type: storage_type):
        storage = self.get_kv_storage(storage_type)
        self.assertEqual(0, len(list(storage.keys())))
        data = "This is some data."
        key1 = "a-b-123"
        key2 = "a-c-123"

        storage.set(key1, data)
        self.assertEqual(data, storage.get(key1))
        storage.set(key2, data)
        self.assertEqual(data, storage.get(key2))
        self.assertEqual({key1, key2}, set(storage.keys()))
        storage.delete(key1)
        self.assertEqual({key2}, set(storage.keys()))
        self.assertFalse(storage.has(key1))
        self.assertTrue(storage.has(key2))
        self.assertFalse(storage.has(data))

    @parameterized.expand(kv_types, skip_on_empty=True)
    def test_kv_storage_special(self, storage_type: storage_type):
        KEYS = ["", "1", "COM", "a.b.c"]
        storage = self.get_kv_storage(storage_type)
        self.assertEqual(0, len(set(storage.keys())))
        data = "Some data."
        for key in KEYS:
            storage.set(key, data)
            self.assertEqual({key}, set(storage.keys()))
            self.assertEqual(data, storage.get(key))
            self.assertTrue(storage.has(key))
            storage.delete(key)
    
    def test_x(self):
        pass
