# import numpy as np
import pandas as pd


class PandasUtil():
    @classmethod
    def fix_dataframe(self, df, type_castings={},
                      datetime_fields=[], datetime_format=None,
                      column_schema={},
                      drop_timezones=False):
        """To do fixes like date formats, type_castings and empty columns

        Args:
            df (pandas dataframe): Dataframe to apply fixes on.
                Will be cloned and fixes would be applied on the clone
            type_castings (dict): Field and new types to which the columns
                have to be casted to. eg.,
                    {"cast_column_1", "string", "cast_column_2", "string"}
                will raise exception if to_cast type is wrong
            datetime_fields (list_of_str): Column names of type datetime
                Only needed if datetime_format is specified. eg.,
                    ["date_column_1", "date_column_2"]
            datetime_format (str): datetime format in which the string date
                fields have to be parsed from. eg.,
                    "%a, %d %b %Y %H:%M:%S %Z"
                    for 'Fri, 03 Aug 2018 03:18:40 GMT'
                Data that doesn't follow the format will be converted as null
            column_schema (dict): A schema of columne_name -> type,
                if any of the provided columns are not found,
                this will help create a null column for it. eg.,
                    {
                        "column_name_1": "string",
                        "column_name_2": "date",
                        "column_name_3": "int",
                        ..etc
                    }
            drop_timezones (bool): If True, will all reference to timezones
                in datetime columne with timezones. Default is False.

        Returns: (pandas.Dataframe)
            returns pandas dataframe with all fixes
        """
        _df = df.copy()
        if column_schema:
            self._fix_missing_columns(_df, column_schema)
        self._cast_types(_df, type_castings)
        if datetime_format:
            self._fix_datetime(
                _df, datetime_fields, datetime_format)
        if drop_timezones:
            self._drop_timezones(_df)
        return _df

    @classmethod
    def _cast_types(self, df, type_castings):
        for _name, _type in type_castings.items():
            if _type == "string":
                df[[_name]] = df[[_name]].astype(str)
            else:
                raise Exception("Type cast not implemented for type %s"
                                % _type)

    @classmethod
    def _fix_datetime(self, df, datetime_fields, datetime_format):
        for _name in datetime_fields:
            df[_name] = pd.to_datetime(
                df[_name], format=datetime_format, errors="coerce")

    @classmethod
    def _fix_missing_columns(self, df, column_schema):
        found_cols = df.columns.tolist()
        for col_name, col_type in column_schema.items():
            if col_name not in found_cols:
                # Empty feilds can be string since pyarrow would fix it
                df[col_name] = pd.Series(dtype=str)

    @classmethod
    def _drop_timezones(self, df):
        datetime_fields_with_tz = [
            k for k, v in df.dtypes.iteritems() if "M8[ns]" in v.str]
        for dt_field in datetime_fields_with_tz:
            df[dt_field] = df[dt_field].dt.tz_localize(None)
