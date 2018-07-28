import os
import uuid
import time
import logging
import traceback
import pandas as pd

import sqlalchemy
from bqsqoop.utils import parquet_util


def get_results_cursor(sql_bind, query, pool_timeout=300):
    engine = sqlalchemy.create_engine(sql_bind, pool_timeout=pool_timeout)
    connection = engine.connect()
    return connection.execute(query)


def export_to_parquet(worker_id, sql_bind, query, filter_field, start_pos,
                      end_pos, output_folder, progress_bar=True,
                      fetch_size=1000):
    try:
        start_time = int(time.time())
        output_file = os.path.join(output_folder, "{}.parq".format(
            str(uuid.uuid4())[:8]))
        if start_pos and end_pos:
            filter_query = "{0} >= {1} AND {0} <= {2}".format(
                filter_field, start_pos, end_pos)
            query = query % filter_query
        parquetUtil = parquet_util.ParquetUtil(output_file)
        engine = sqlalchemy.create_engine(sql_bind, pool_timeout=300)
        connection = engine.connect()
        for chunk in pd.read_sql_query(
            sql=query,
            con=connection,
            chunksize=fetch_size
        ):
            parquetUtil.append_df_to_parquet(chunk)
        parquetUtil.close()
        logging.info(
            "Dump -> worker_id: {0}, time_taken: {1} secs".format(
                worker_id, int(time.time()) - start_time))
        return output_file
    except Exception:
        logging.error(traceback.format_exc())
        raise
