import os
import unittest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from datetime import datetime
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

    def test_build_pyarrow_schema(self):
        _pu = ParquetUtil("tmp")
        column_schema = {
            "string_field": "string",
            "str_field": "str",
            "text_field": "text",
            "integer_field": "integer",
            "long_field": "long",
            "float_field": "float",
            "bool_field": "bool",
            "date_field": "date",
            "datetime_field": "datetime"
        }
        pa_schema = _pu.build_pyarrow_schema(column_schema)
        self.assertEqual(pa_schema.field_by_name(
            "string_field").type, pa.string())
        self.assertEqual(pa_schema.field_by_name(
            "str_field").type, pa.string())
        self.assertEqual(pa_schema.field_by_name(
            "text_field").type, pa.string())
        self.assertEqual(pa_schema.field_by_name(
            "integer_field").type, pa.int64())
        self.assertEqual(pa_schema.field_by_name(
            "long_field").type, pa.int64())
        self.assertEqual(pa_schema.field_by_name(
            "float_field").type, pa.float64())
        self.assertEqual(pa_schema.field_by_name(
            "bool_field").type, pa.binary())
        self.assertEqual(pa_schema.field_by_name(
            "date_field").type, pa.timestamp('ns'))
        self.assertEqual(pa_schema.field_by_name(
            "datetime_field").type, pa.timestamp('ns'))

    def test_fix_dataframe_for_schema(self):
        data = [
            dict(colA=None, colB=None, colD=1,
                 colE=datetime(2018, 9, 10, 0, 0, 0, 100)),
            dict(colA=None, colB=None, colD=3)
        ]
        df = pd.DataFrame.from_dict(data)
        arrow_schema = [
            pa.field(
                name="colA",
                type=pa.string(),
                nullable=True),
            pa.field(
                name="colB",
                type=pa.float64(),
                nullable=True),
            pa.field(
                name="colC",
                type=pa.binary(),
                nullable=True),
            pa.field(
                name="colD",
                type=pa.int64(),
                nullable=True),
            pa.field(
                name="colE",
                type=pa.timestamp('ns'),
                nullable=True)
        ]
        result_df = ParquetUtil.fix_dataframe_for_schema(df, arrow_schema)
        self.assertEqual(result_df["colA"].dtypes, object)
        self.assertEqual(result_df["colB"].dtypes, float)
        self.assertEqual(result_df["colC"].dtypes, bool)
        self.assertEqual(result_df["colD"].dtypes, int)
        self.assertEqual(result_df["colE"].dtypes, "<M8[ns]")
