import os
import uuid
import time
import json
import logging
import traceback
import sqlalchemy
import pandas as pd

from datetime import datetime
from bqsqoop.utils import parquet_util


def get_results_cursor(sql_bind, query, pool_timeout=300):
    engine = sqlalchemy.create_engine(sql_bind, pool_timeout=pool_timeout)
    connection = engine.connect()
    return connection.execute(query)


def get_streaming_result_proxy(sql_bind, query, pool_timeout=300):
    engine = sqlalchemy.create_engine(sql_bind, pool_timeout=pool_timeout)
    connection = engine.connect()
    proxy = connection.execution_options(
        stream_results=True).execute(query)
    return proxy


def get_table_schema(sql_bind, table_name, pool_timeout=300):
    engine = sqlalchemy.create_engine(sql_bind, pool_timeout=pool_timeout)
    insp = sqlalchemy.inspect(engine)
    columns = insp.get_columns(table_name)
    table_schema = {}
    for column in columns:
        table_schema[column["name"]] = column["type"].python_type
    return table_schema


def export_to_parquet(worker_id, sql_bind, query, filter_field, start_pos,
                      end_pos, output_folder, progress_bar=True,
                      fetch_size=100, table_schema=None):
    try:
        start_time = int(time.time())
        output_file = os.path.join(output_folder, "{}.parq".format(
            str(uuid.uuid4())[:8]))
        if start_pos and end_pos:
            filter_query = "{0} >= {1} AND {0} <= {2}".format(
                filter_field, start_pos, end_pos)
            query = query % filter_query
        parquetUtil = parquet_util.ParquetUtil(output_file)
        parquet_schema = None
        if table_schema:
            parquet_schema = parquetUtil.build_pyarrow_schema(table_schema)
        logging.debug(output_file)
        logging.debug(query)
        proxy = get_streaming_result_proxy(sql_bind, query)
        results = proxy.fetchmany(fetch_size)
        columns = [key[0] for key in proxy.cursor.description]
        while True:
            if results:
                items = []
                for row in results:
                    row_dict = {}
                    for x, y in zip(columns, row):
                        row_dict[x] = _data_type_transform(y)
                    items.append(row_dict)
                df = pd.DataFrame.from_dict(items)
                df = parquet_util.ParquetUtil.fix_dataframe_for_schema(
                    df, parquet_schema)
                parquetUtil.append_df_to_parquet(df, schema=parquet_schema)
                results = proxy.fetchmany(fetch_size)
            else:
                break
        parquetUtil.close()
        logging.info(
            "Dump -> worker_id: {0}, time_taken: {1} secs".format(
                worker_id, int(time.time()) - start_time))
        return output_file
    except Exception:
        logging.error(traceback.format_exc())
        raise


def _data_type_transform(value, compound_types_to_json_string=True):
    value_transformed = value
    if type(value) is datetime:
        if(value.utcoffset()):  # if timestamp is given with timezone
            value_transformed = value.replace(tzinfo=None) - value.utcoffset()
        else:   # time is already in UTC
            value_transformed = value.replace(tzinfo=None)
    elif compound_types_to_json_string and \
            isinstance(value, (dict, list, tuple)):
        value_transformed = json.dumps(value)
    return value_transformed
