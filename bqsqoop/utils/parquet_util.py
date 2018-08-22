import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal
from datetime import datetime
from bqsqoop.utils.pandas_util import PandasUtil


class ParquetUtil():
    def __init__(self, output_file):
        """Helper for writing Parquet files

        Args:
            output_file (str): output file name with full path
        """
        self._output_file = output_file
        self._pqwriter = None

    @classmethod
    def fix_dataframe_for_schema(self, df, arrow_schema, datetime_format=None):
        """Fixes pandas DF for arrow format

        Since arrow has strict types and pandas is not so,
        this function can help fix some of those issues.

        Args:
            df (obj): A pandas dataframe
            arrow_schema (dict): A pyarrow_schema for the dataframe,
                `build_pyarrow_schema` method returns the right format

        Returns (obj):
            Fixed pandas dataframe
        """
        pandasUtil = PandasUtil(datetime_format=datetime_format)
        df_fix_fns = {
            pa.string(): pandasUtil.fix_string,
            pa.binary(): pandasUtil.fix_bool,
            pa.float64(): pandasUtil.fix_float,
            pa.int64(): pandasUtil.fix_int,
            pa.timestamp('ns'): pandasUtil.fix_timestamp,
        }
        _df = df.copy()
        for k, v in arrow_schema.items():
            if v in df_fix_fns:
                fn = df_fix_fns[v]
                if k not in _df.columns:
                    _df[k] = pd.Series()
                _df[k] = fn(_df[k])
        return _df

    def build_pyarrow_schema(self, column_schema):
        """Convert columnname-type KV pairs to pyarrow.schema

        Args:
            column_schema (dict): A schema of columne_name -> type,
                if any of the provided columns are not found,
                this will help create a null column for it. eg.,
                    {
                        "column_name_1": "string",
                        "column_name_2": "date",
                        "column_name_3": "int",
                        ..etc
                    }

        Returns: (pyarrow.Schema)
            returns built schema for pyarrow
        """
        fields = []
        column_types_map = {
            "string": pa.string(),
            "str": pa.string(),
            "text": pa.string(),
            "integer": pa.int64(),
            "long": pa.int64(),
            "float": pa.float64(),
            "bool": pa.binary(),
            "boolean": pa.binary(),
            "date": pa.timestamp('ns'),
            "datetime": pa.timestamp('ns'),
            # For python types
            int: pa.int64(),
            Decimal: pa.decimal128(7),
            float: pa.float64(),
            str: pa.string(),
            datetime: pa.timestamp('ns'),
            bool: pa.bool_(),
        }
        for col_name, col_type in column_schema.items():
            fields.append(
                pa.field(
                    name=col_name,
                    type=column_types_map.get(col_type, pa.string()),
                    nullable=True))
        return pa.schema(fields=fields)

    def write_df_to_parquet(self, df, preserve_index=False, close_writer=True,
                            schema=None):
        """Writes Pandas Dataframe to a new Parquet file

        Closes the writer after writing all of dataframe and before returning

        Args:
            df (Pandas Datafram): Data to be written to parquet file.
            preserve_index (bool): Set this to True if you want to
                preserve the index of pandas dataframe, default is False
                and indexes are dropped.
            close_writer (bool): Set to true closes the writer at the end
                Default: True
            schema (pyarrow.Schema, optional): The expected schema of the
                Arrow Table. This can be used to indicate the type of columns
                if we cannot infer it automatically.
        """
        table = pa.Table.from_pandas(df, preserve_index=preserve_index,
                                     schema=schema)
        self._pqwriter = pq.ParquetWriter(self._output_file, table.schema)
        self._pqwriter.write_table(table)
        if close_writer:
            self.close()

    def append_df_to_parquet(self, df, preserve_index=False, schema=None):
        """Writes Pandas Dataframe to a new Parquet file in Append mode

        Can be used to append data to the same parquet file multiple times,
        need to call close() funciton after done with all appends.

        Args:
            df (Pandas Datafram): Data to be written to parquet file.
            preserve_index (bool): Set this to True if you want to
                preserve the index of pandas dataframe, default is False
                and indexes are dropped.
            schema (pyarrow.Schema, optional): The expected schema of the
                Arrow Table. This can be used to indicate the type of columns
                if we cannot infer it automatically.
        """
        if not self._pqwriter:
            self.write_df_to_parquet(df, preserve_index=preserve_index,
                                     close_writer=False, schema=schema)
        else:
            table = pa.Table.from_pandas(
                df, preserve_index=preserve_index, schema=schema)
            self._pqwriter.write_table(table)

    def close(self):
        """Closes the parquet writer to the output file

        It's safe to call it multiple times, will only close if a writer
        is open.
        """
        if self._pqwriter:
            self._pqwriter.close()
            self._pqwriter = None
