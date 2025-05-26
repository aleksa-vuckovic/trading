from __future__ import annotations
from parameterized import parameterized
from base.tests.common import A, TestPersistence, storage_type, ks_types

class TestKSStorage(TestPersistence):
    
    @parameterized.expand(ks_types)
    def test_key_series_storage(self, storage_type: storage_type):
        storage = self.get_ks_storage(storage_type)
        self.assertEqual(0, len(list(storage.keys())))
        KEY1 = "key1"
        KEY2 = "key2"
        data_100_200 = [A(100+i,i) for i in range(1,101)]
        storage.set(KEY1, data_100_200)
        self.assertEqual(data_100_200, storage.get(KEY1, 0, 300)) #Persistence
        self.assertEqual(data_100_200, storage.get(KEY1, 100, 200)) #Boundry inclusivity
        storage.delete(KEY1, 150, 160) # deletion
        data_100_200 = [it for it in data_100_200 if it.t <= 150 or it.t > 160]
        self.assertEqual(data_100_200, storage.get(KEY1, 0, 300))
        
        storage.set(KEY2, [A(450, 50)]) #multiple keys
        self.assertEqual([A(450,50)], storage.get(KEY2, 420, 470))
        self.assertEqual(data_100_200, storage.get(KEY1, 100, 200))
        self.assertEqual({KEY1, KEY2}, set(storage.keys()))
