# import numpy as np
import pandas as pd


class PandasUtil():
    def __init__(self, datetime_format=None):
        self.datetime_format = datetime_format

    def fix_string(self, series):
        return series.astype(str)

    def fix_bool(self, series):
        return series.astype(bool)

    def fix_float(self, series):
        return pd.to_numeric(series)

    def fix_int(self, series):
        # Cause int's can't have null values
        return series.astype(str)

    def fix_timestamp(self, series):
        if self.datetime_format:
            series = pd.to_datetime(
                series, format=self.datetime_format, errors="coerce")
        else:
            series = pd.to_datetime(series, errors="coerce")
        # Drop the timezone if any
        series = series.dt.tz_localize(None)
        return series
