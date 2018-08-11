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
                      fetch_size=100):
    try:
        start_time = int(time.time())
        output_file = os.path.join(output_folder, "{}.parq".format(
            str(uuid.uuid4())[:8]))
        if start_pos and end_pos:
            filter_query = "{0} >= {1} AND {0} <= {2}".format(
                filter_field, start_pos, end_pos)
            query = query % filter_query
        parquetUtil = parquet_util.ParquetUtil(output_file)
        logging.debug(output_file)
        logging.debug(query)
        engine = sqlalchemy.create_engine(sql_bind, pool_timeout=3000)
        connection = engine.connect()
        proxy = connection.execution_options(
            stream_results=True).execute(query)
        results = proxy.fetchmany(fetch_size)
        logging.debug(results)
        while True:
            if results:
                items = []
                # for result in results:
                items = ([dict(zip([key[0] for key in proxy.cursor.description], row)) for row in results])
                logging.debug(items)
                df = pd.DataFrame.from_dict(items)
                parquetUtil.append_df_to_parquet(df)
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
