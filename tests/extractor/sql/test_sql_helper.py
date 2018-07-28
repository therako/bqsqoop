import pytest
import unittest
import pandas as pd

from unittest.mock import patch, MagicMock
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
    @patch('pandas.read_sql_query')
    @patch('sqlalchemy.create_engine')
    def test_export_to_parquet(self, create_engine, read_sql_query,
                               parquet_util, uuid):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection

        read_sql_query.return_value = iter([
            pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]}),
            pd.DataFrame(data={'col1': [5, 6], 'col2': [7, 8]})
        ])

        mock_parquet_util = MagicMock()
        parquet_util.return_value = mock_parquet_util

        output_file = export_to_parquet(
            worker_id=1, sql_bind="sql_bind", query="query %s",
            filter_field="filter_field", start_pos=34,
            end_pos=402, output_folder="output_folder/",
            progress_bar=False, fetch_size=100
        )
        self.assertEqual(output_file,
                         "output_folder/F43C2651.parq")
        create_engine.assert_called_with("sql_bind", pool_timeout=300)
        mock_engine.connect.assert_called_with()
        read_sql_query.assert_called_with(
            sql="query filter_field >= 34 AND filter_field <= 402",
            con=mock_connection,
            chunksize=100
        )
        call_list = mock_parquet_util.append_df_to_parquet.call_args_list
        print(call_list)
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
    @patch('pandas.read_sql_query')
    @patch('sqlalchemy.create_engine')
    def test_export_to_parquet_with_no_start_and_end_filters(
            self, create_engine, read_sql_query, parquet_util, uuid):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection

        read_sql_query.return_value = iter([
            pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]}),
            pd.DataFrame(data={'col1': [5, 6], 'col2': [7, 8]})
        ])

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
        read_sql_query.assert_called_with(
            sql="query",
            con=mock_connection,
            chunksize=1000
        )
        call_list = mock_parquet_util.append_df_to_parquet.call_args_list
        print(call_list)
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
    @patch('pandas.read_sql_query')
    @patch('sqlalchemy.create_engine')
    def test_export_to_parquet_exceptions(
            self, create_engine, read_sql_query, parquet_util, uuid):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection

        read_sql_query.side_effect = Exception('Test error')

        mock_parquet_util = MagicMock()
        parquet_util.return_value = mock_parquet_util

        with pytest.raises(Exception, match=r"Test error"):
            export_to_parquet(
                worker_id=1, sql_bind="sql_bind", query="query",
                filter_field=None, start_pos=None,
                end_pos=None, output_folder="output_folder/",
                progress_bar=False
            )
