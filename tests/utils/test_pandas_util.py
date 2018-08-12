import pytz
import pytest
import unittest
import pandas as pd

from datetime import datetime, timezone
from bqsqoop.utils.pandas_util import PandasUtil


class TestPandasUtil(unittest.TestCase):
    def test_no_fixes_case(self):
        data = [
            dict(colA="val1", colB=1),
            dict(colA="val2", colB=2)
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(df)
        self.assertEqual(df_after_fix.to_json(
        ), '{"colA":{"0":"val1","1":"val2"},"colB":{"0":1,"1":2}}')

    def test_fix_datetime_fields(self):
        data = [
            dict(colA='Fri, 03 Aug 2018 03:18:40 GMT', colB=1,
                 colC='Sat, 04 Aug 2018 03:18:40 GMT'),
            dict(colA='Fri, 03 Aug 2018 05:18:40 GMT', colB=2,
                 colC='Invalid datetime format')
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(
            df, datetime_fields=["colA", "colC"],
            datetime_format="%a, %d %b %Y %H:%M:%S %Z"
        )
        self.assertEqual(df_after_fix.to_json(
        ), '{"colA":{"0":1533266320000,"1":1533273520000},"colB":{"0":1,"1":2},"colC":{"0":1533352720000,"1":null}}')   # noqa

    def test_type_castings(self):
        data = [
            dict(colA=2, colB=1, colC=3),
            dict(colA=6, colB=5, colC=4),
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(
            df, type_castings={"colA": "string", "colC": "string"}
        )
        self.assertEqual(df_after_fix.to_json(
        ), '{"colA":{"0":"2","1":"6"},"colB":{"0":1,"1":5},"colC":{"0":"3","1":"4"}}')   # noqa

    def test_type_castings_unkown_type(self):
        data = [
            dict(colA=2, colB=1, colC=3),
            dict(colA=6, colB=5, colC=4),
        ]
        df = pd.DataFrame.from_dict(data)
        with pytest.raises(
                Exception,
                match=r'Type cast not implemented for type wrong_type'):
            PandasUtil.fix_dataframe(
                df, type_castings={"colA": "wrong_type", "colC": "string"}
            )

    def test_add_missing_columns(self):
        data = [
            dict(colA=2),
            dict(colA=6),
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(
            df, column_schema={
                "colA": "string",
                "colB": "text",
                "colE": "integer"
            }
        )
        self.assertEqual(df_after_fix.columns.tolist(),
                         ['colA', 'colB', 'colE'])
        self.assertEqual(df_after_fix["colA"][0], 2)
        self.assertEqual(df_after_fix["colA"][1], 6)
        self.check_missing_col(df_after_fix, "colB", "object")
        self.check_missing_col(df_after_fix, "colE", "object")

    def check_missing_col(self, df, col_name, col_type):
        self.assertEqual(df[col_name].dtype.name, col_type)
        self.assertTrue(pd.isnull(df[col_name][0]))
        self.assertTrue(pd.isnull(df[col_name][1]))

    def test_drop_timezones(self):
        utc_dt = pytz.utc.localize(datetime(2018, 1, 1, 1, 0, 0))
        pst_dt = utc_dt.astimezone(pytz.timezone("America/Los_Angeles"))
        data = [
            dict(
                colA=pst_dt,
                colB=datetime(2018, 1, 1, tzinfo=None),
                colC=3),
            dict(
                colA=pst_dt,
                colB=datetime(2018, 3, 1, tzinfo=None),
                colC=6),
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(df, drop_timezones=True)
        self.assertEqual(df_after_fix.columns.tolist(),
                         ['colA', 'colB', 'colC'])
        self.assertEqual(str(df_after_fix["colA"][0]), "2017-12-31 17:00:00")
        self.assertEqual(str(df_after_fix["colB"][0]), "2018-01-01 00:00:00")
        self.assertEqual(df_after_fix["colC"][0], 3)
        self.assertEqual(str(df_after_fix["colA"][1]), "2017-12-31 17:00:00")
        self.assertEqual(str(df_after_fix["colB"][1]), "2018-03-01 00:00:00")
        self.assertEqual(df_after_fix["colC"][1], 6)
