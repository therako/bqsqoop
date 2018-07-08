import os
import unittest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bqsqoop.utils.parquet_util import ParquetUtil


def sample_df():
    _data = [
        dict(colA="val1", colB=1),
        dict(colA="val2", colB=2)
    ]
    return pd.DataFrame.from_dict(_data)


class TestParquetUtil(unittest.TestCase):
    def test_write_df_to_parquet(self):
        _filename = "/tmp/test_write_df_to_parquet.parq"
        _pu = ParquetUtil(_filename)
        _pu.write_df_to_parquet(sample_df())
        _pu.close()

        _ptable = pq.read_table(_filename)
        os.remove(_filename)
        self.assertIn(_ptable.schema.field_by_name(
            "colA").type, [pa.string(), pa.binary()])
        self.assertEqual(_ptable.schema.field_by_name("colB").type, pa.int64())
        _table_dict = _ptable.to_pydict()
        self.assertEqual(_table_dict['colA'], ['val1', 'val2'])
        self.assertEqual(_table_dict['colB'], [1, 2])

    def test_append_df_to_parquet(self):
        _filename = "/tmp/test_append_df_to_parquet.parq"
        _pu = ParquetUtil(_filename)
        _pu.append_df_to_parquet(sample_df())
        _pu.append_df_to_parquet(sample_df())
        _pu.close()

        _ptable = pq.read_table(_filename)
        os.remove(_filename)
        self.assertIn(_ptable.schema.field_by_name(
            "colA").type, [pa.string(), pa.binary()])
        self.assertEqual(_ptable.schema.field_by_name("colB").type, pa.int64())
        _table_dict = _ptable.to_pydict()
        self.assertEqual(_table_dict['colA'], ['val1', 'val2', 'val1', 'val2'])
        self.assertEqual(_table_dict['colB'], [1, 2, 1, 2])
