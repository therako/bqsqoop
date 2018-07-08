from time import sleep
import unittest

from bqsqoop.utils.async_worker import AsyncWorker


def sleep_task(ms):
    sleep(ms / 1000)
    return ms


def raise_exception():
    raise Exception("Test")


class TestAsyncWorker(unittest.TestCase):
    def test_happy_path(self):
        _worker = AsyncWorker(2)
        _worker.send_data_to_worker(sleep_task, ms=5)
        _worker.send_data_to_worker(sleep_task, ms=1)
        _results = _worker.get_job_results()
        self.assertTrue(1 in _results)
        self.assertTrue(5 in _results)

    def test_exception_path(self):
        _worker = AsyncWorker(2)
        _worker.send_data_to_worker(sleep_task, ms=5)
        _worker.send_data_to_worker(raise_exception)
        with self.assertRaises(Exception):
            _worker.get_job_results()
