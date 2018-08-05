import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetUtil():
    def __init__(self, output_file):
        """Helper for writing Parquet files

        Args:
            output_file (str): output file name with full path
        """
        self._output_file = output_file
        self._pqwriter = None

    def write_df_to_parquet(self, df, preserve_index=False, close_writer=True):
        """Writes Pandas Dataframe to a new Parquet file

        Closes the writer after writing all of dataframe and before returning

        Args:
            df (Pandas Datafram): Data to be written to parquet file.
            preserve_index (bool): Set this to True if you want to
                preserve the index of pandas dataframe, default is False
                and indexes are dropped.
            close_writer (bool): Set to true closes the writer at the end
                Default: True
        """
        table = pa.Table.from_pandas(df, preserve_index=preserve_index)
        self._pqwriter = pq.ParquetWriter(self._output_file, table.schema)
        self._pqwriter.write_table(table)
        if close_writer:
            self.close()

    def append_df_to_parquet(self, df, preserve_index=False):
        """Writes Pandas Dataframe to a new Parquet file in Append mode

        Can be used to append data to the same parquet file multiple times,
        need to call close() funciton after done with all appends.

        Args:
            df (Pandas Datafram): Data to be written to parquet file.
            preserve_index (bool): Set this to True if you want to
                preserve the index of pandas dataframe, default is False
                and indexes are dropped.
        """
        if not self._pqwriter:
            self.write_df_to_parquet(df, preserve_index=preserve_index,
                                     close_writer=False)
        else:
            table = pa.Table.from_pandas(
                df, preserve_index=preserve_index)
            self._pqwriter.write_table(table)

    def close(self):
        """Closes the parquet writer to the output file

        It's safe to call it multiple times, will only close if a writer
        is open.
        """
        if self._pqwriter:
            self._pqwriter.close()
            self._pqwriter = None

    def fix_dataframe(self, df, type_castings={},
                      datetime_fields=[], datetime_format=None,
                      column_schema={}):
        """To do fixes like data types, type_castings and empty columns
        """
        _df = df.copy()
        self._cast_types(_df, type_castings)
        if datetime_format:
            self._fix_datetime(
                _df, datetime_fields, datetime_format)
        if column_schema:
            self._fix_missing_columns(_df, column_schema)
        return _df

    def _cast_types(self, df, type_castings):
        for _name, _type in type_castings.items():
            if _type == "string":
                df[[_name]] = df[[_name]].astype(str)
            else:
                raise Exception("Type cast not implemented for type %s"
                                % _type)

    def _fix_datetime(self, df, datetime_fields, datetime_format):
        for _name in datetime_fields:
            df[_name] = pd.to_datetime(
                df[_name], format=datetime_format, errors="coerce")

    def _fix_missing_columns(self, df, column_schema):
        pass
