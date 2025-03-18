import unittest
from trading.providers.utils import combine_series, filter_by_timestamp

class TestUtils(unittest.TestCase):

    def test_combine_series_as_dict(self):
        input = {key: [
            None if key == 'd' and i == 3
            else 0 if key=='Aaa' and i == 4
            else None if key == 'b' and i == 5
            else i + index for i in range(1,6)
            ] for index,key in enumerate(['Aaa', 'b', 'c', 'd'])}
        expect = [{'a': i, 'b': None if i == 5 else i+1, 'c': i+2, 'd': i+3} for i in range(1,6) if i != 3 and i != 4]
        result = combine_series(input, must_be_truthy=['a','d'], must_be_there=['a','b','c','d'])
        self.assertEqual(expect, result)

        expect = expect[:-1]
        result = combine_series(input, must_be_there=['a','b','c','d'])
        self.assertEqual(expect, result)

    def test_combine_series_as_list(self):
        input = {key: [
            None if key == 'd' and i == 3
            else 0 if key=='a' and i == 4
            else None if key == 'b' and i == 5
            else i + index for i in range(1,6)
            ] for index,key in enumerate(['a', 'b', 'c', 'd'])}
        expect = [[None if i == 5 else i+1, i, i+2] for i in range(1,6) if i != 3 and i != 4]
        result = combine_series(input, must_be_there=['b','a','c'], must_be_truthy=['a','d'], as_list=True)
        self.assertEqual(expect, result)

    def test_filter_by_timestamp(self):
        input = [{'t': 5}, {'t':1}, {'t': 3}, {'t': 3}, {'t': 10}, {'t': 11}]
        expect = [{'t': 3}, {'t': 5}, {'t': 10}]
        result = filter_by_timestamp(input, unix_from=1, unix_to=10)
        self.assertEqual(expect, result)