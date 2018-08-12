import pytz
import pytest
import unittest
import pandas as pd

from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta
from bqsqoop.extractor.sql.helper import (
    get_results_cursor, export_to_parquet
)


class TestSQLHelper(unittest.TestCase):
    @patch('sqlalchemy.create_engine')
    def test_get_results_cursor(self, create_engine):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection
        expected_result = ["some value"]
        mock_connection.execute.return_value = expected_result
        result = get_results_cursor("sql_bind1", "select *", pool_timeout=500)
        create_engine.assert_called_with("sql_bind1", pool_timeout=500)
        mock_engine.connect.assert_called_with()
        mock_connection.execute.assert_called_with("select *")
        self.assertEqual(expected_result, result)

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('sqlalchemy.create_engine')
    def test_export_to_parquet(self, create_engine, parquet_util, uuid):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection
        mock_connection.execution_options.return_value = mock_connection
        mock_proxy = MagicMock()
        mock_connection.execute.return_value = mock_proxy
        mock_proxy.fetchmany.side_effect = [
            [[1, 3], [2, 4]],
            [[5, 7], [6, 8]],
            None
        ]
        mock_proxy.cursor.description = [
            ('col1', "info"),
            ('col2', "info")
        ]
        mock_parquet_util = MagicMock()
        parquet_util.return_value = mock_parquet_util

        output_file = export_to_parquet(
            worker_id=1, sql_bind="sql_bind", query="query %s",
            filter_field="filter_field", start_pos=34,
            end_pos=402, output_folder="output_folder/",
            progress_bar=False, fetch_size=200
        )
        self.assertEqual(output_file,
                         "output_folder/F43C2651.parq")
        create_engine.assert_called_with("sql_bind", pool_timeout=300)
        mock_engine.connect.assert_called_with()
        mock_connection.execution_options.assert_called_with(
            stream_results=True)
        mock_connection.execute.assert_called_with(
            "query filter_field >= 34 AND filter_field <= 402")
        mock_proxy.fetchmany.assert_has_calls(
            [call(200), call(200), call(200)])
        call_list = mock_parquet_util.append_df_to_parquet.call_args_list
        args, _ = call_list[0]
        df = args[0]
        self.assertEqual(
            df.to_json(), '{"col1":{"0":1,"1":2},"col2":{"0":3,"1":4}}')
        args, _ = call_list[1]
        df = args[0]
        self.assertEqual(
            df.to_json(), '{"col1":{"0":5,"1":6},"col2":{"0":7,"1":8}}')

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('sqlalchemy.create_engine')
    def test_export_to_parquet_with_no_start_and_end_filters(
            self, create_engine, parquet_util, uuid):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection
        mock_connection.execution_options.return_value = mock_connection
        mock_proxy = MagicMock()
        mock_connection.execute.return_value = mock_proxy
        mock_proxy.fetchmany.side_effect = [
            [[1, 3], [2, 4]],
            [[5, 7], [6, 8]],
            None
        ]
        mock_proxy.cursor.description = [
            ('col1', "info"),
            ('col2', "info")
        ]
        mock_parquet_util = MagicMock()
        parquet_util.return_value = mock_parquet_util

        output_file = export_to_parquet(
            worker_id=1, sql_bind="sql_bind", query="query",
            filter_field=None, start_pos=None,
            end_pos=None, output_folder="output_folder/",
            progress_bar=False
        )
        self.assertEqual(output_file,
                         "output_folder/F43C2651.parq")
        create_engine.assert_called_with("sql_bind", pool_timeout=300)
        mock_engine.connect.assert_called_with()
        mock_connection.execution_options.assert_called_with(
            stream_results=True)
        mock_connection.execute.assert_called_with("query")
        mock_proxy.fetchmany.assert_has_calls(
            [call(100), call(100), call(100)])
        call_list = mock_parquet_util.append_df_to_parquet.call_args_list
        args, _ = call_list[0]
        df = args[0]
        self.assertEqual(
            df.to_json(), '{"col1":{"0":1,"1":2},"col2":{"0":3,"1":4}}')
        args, _ = call_list[1]
        df = args[0]
        self.assertEqual(
            df.to_json(), '{"col1":{"0":5,"1":6},"col2":{"0":7,"1":8}}')

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('sqlalchemy.create_engine')
    def test_export_to_parquet_exceptions(
            self, create_engine, parquet_util, uuid):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection
        mock_connection.execution_options.return_value = mock_connection
        mock_connection.execute.side_effect = Exception('Test error')

        mock_parquet_util = MagicMock()
        parquet_util.return_value = mock_parquet_util

        with pytest.raises(Exception, match=r"Test error"):
            export_to_parquet(
                worker_id=1, sql_bind="sql_bind", query="query",
                filter_field=None, start_pos=None,
                end_pos=None, output_folder="output_folder/",
                progress_bar=False
            )

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('sqlalchemy.create_engine')
    def test_data_type_transform(self, create_engine, parquet_util, uuid):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection
        mock_connection.execution_options.return_value = mock_connection
        mock_proxy = MagicMock()
        mock_connection.execute.return_value = mock_proxy
        utc_dt = pytz.utc.localize(datetime(2018, 1, 1, 1, 0, 0))
        pst_dt = utc_dt.astimezone(pytz.timezone("America/Los_Angeles"))
        dict_type = {"a": 2}
        list_type = [2, 3]
        tuple_type = ("sfe", 7)
        mock_proxy.fetchmany.side_effect = [
            [[pst_dt, utc_dt, dict_type, list_type, tuple_type], [
                pst_dt + timedelta(days=1), utc_dt + timedelta(days=1),
                dict_type, list_type, tuple_type]],
            None
        ]
        mock_proxy.cursor.description = [
            ('pst_dt', "info"),
            ('utc_dt', "info"),
            ('dict_type', "info"),
            ('list_type', "info"),
            ('tuple_type', "info")
        ]
        mock_parquet_util = MagicMock()
        parquet_util.return_value = mock_parquet_util

        export_to_parquet(
            worker_id=1, sql_bind="sql_bind", query="query",
            filter_field=None, start_pos=None,
            end_pos=None, output_folder="output_folder/",
            progress_bar=False
        )
        call_list = mock_parquet_util.append_df_to_parquet.call_args_list
        args, _ = call_list[0]
        df = args[0]
        # Both datetime fields should be in UTC
        self.assertEqual(
            df["pst_dt"].tolist(), [
                pd.Timestamp('2018-01-01 01:00:00'),
                pd.Timestamp('2018-01-02 01:00:00')])
        self.assertEqual(
            df["utc_dt"].tolist(), [
                pd.Timestamp('2018-01-01 01:00:00'),
                pd.Timestamp('2018-01-02 01:00:00')])
        self.assertEqual(
            df["dict_type"].tolist(), ['{"a": 2}', '{"a": 2}'])
        self.assertEqual(
            df["list_type"].tolist(), ['[2, 3]', '[2, 3]'])
        self.assertEqual(
            df["tuple_type"].tolist(), ['["sfe", 7]', '["sfe", 7]'])
