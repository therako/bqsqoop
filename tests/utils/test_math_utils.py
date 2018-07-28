import unittest

from datetime import datetime
from bqsqoop.utils.math_util import calculate_splits


class TestCalculateSplits(unittest.TestCase):
    def test_int_splits(self):
        splits = calculate_splits(3, 267, 10)
        self.assertEqual(len(splits), 10)
        self.assertEqual(splits, [
            {'end': 29, 'start': 3},
            {'end': 56, 'start': 30},
            {'end': 83, 'start': 57},
            {'end': 110, 'start': 84},
            {'end': 137, 'start': 111},
            {'end': 164, 'start': 138},
            {'end': 191, 'start': 165},
            {'end': 218, 'start': 192},
            {'end': 245, 'start': 219},
            {'end': 272, 'start': 246}])

    def test_datetime_splits(self):
        splits = calculate_splits(
            datetime(2017, 1, 1), datetime(2018, 1, 1), 12)
        self.assertEqual(len(splits), 12)
        expected_splits = [
            {
                'start': datetime(2017, 1, 1, 0, 0),
                'end': datetime(2017, 1, 31, 10, 0, 1)
            },
            {
                'start': datetime(2017, 1, 31, 10, 0, 1),
                'end': datetime(2017, 3, 2, 20, 0, 2)
            },
            {
                'start': datetime(2017, 3, 2, 20, 0, 2),
                'end': datetime(2017, 4, 2, 6, 0, 3)
            },
            {
                'start': datetime(2017, 4, 2, 6, 0, 3),
                'end': datetime(2017, 5, 2, 16, 0, 4)
            },
            {
                'start': datetime(2017, 5, 2, 16, 0, 4),
                'end': datetime(2017, 6, 2, 2, 0, 5)
            },
            {
                'start': datetime(2017, 6, 2, 2, 0, 5),
                'end': datetime(2017, 7, 2, 12, 0, 6)
            },
            {
                'start': datetime(2017, 7, 2, 12, 0, 6),
                'end': datetime(2017, 8, 1, 22, 0, 7)
            },
            {
                'start': datetime(2017, 8, 1, 22, 0, 7),
                'end': datetime(2017, 9, 1, 8, 0, 8)
            },
            {
                'start': datetime(2017, 9, 1, 8, 0, 8),
                'end': datetime(2017, 10, 1, 18, 0, 9)
            },
            {
                'start': datetime(2017, 10, 1, 18, 0, 9),
                'end': datetime(2017, 11, 1, 4, 0, 10)
            },
            {
                'start': datetime(2017, 11, 1, 4, 0, 10),
                'end': datetime(2017, 12, 1, 14, 0, 11)
            },
            {
                'start': datetime(2017, 12, 1, 14, 0, 11),
                'end': datetime(2018, 1, 1, 0, 0, 12)
            }]
        self.assertEqual(splits, expected_splits)
