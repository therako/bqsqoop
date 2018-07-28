import logging
import uuid

from bqsqoop.utils import async_worker, math_util
from bqsqoop.extractor import Extractor
from bqsqoop.utils.errors import MissingConfigError
from bqsqoop.extractor.sql import helper


class SQLExtractor(Extractor):
    """Extractor data from sql based DB

    Any DB supported by sqlalchemy should work

    """

    def __init__(self, _config):
        super().__init__(_config)
        self._no_of_workers = self._config.get('no_of_workers', 1)
        self._output_folder = self._config.get(
            'output_folder', "./" + str(uuid.uuid4())[:8])

    def validate_config(self):
        """Validates required configs for SQL extraction
        """
        root_name = 'SQL'
        if 'sql_bind' not in self._config:
            raise MissingConfigError('sql_bind', root_name)
        if 'query' not in self._config:
            raise MissingConfigError('query', root_name)
        if self._no_of_workers > 1 and 'filter_field' not in self._config:
            raise MissingConfigError('filter_field', root_name)
        return None

    def extract_to_parquet(self):
        if self._no_of_workers > 1:
            _async_worker = async_worker.AsyncWorker(
                self._no_of_workers)
            splits = self._add_filter_to_query()
            for i in range(self._no_of_workers):
                _async_worker.send_data_to_worker(
                    worker_id=i,
                    **self._get_extract_job_fn_and_params(splits[i])
                )
            logging.debug('Waiting for Extractor job results...')
            return _async_worker.get_job_results()
        else:
            args = self._get_extract_job_fn_and_params(None)
            del args['worker_callback']
            res = helper.export_to_parquet(
                worker_id=0,
                **args
            )
            return [res]

    def _add_filter_to_query(self):
        min_id, max_id = self._get_min_max()
        splits = math_util.calculate_splits(
            min_id, max_id, self._no_of_workers)
        return splits

    def _get_min_max(self):
        filter_field = self._config['filter_field']
        query = self._config['query']
        sql_bind = self._config['sql_bind']
        sub_mix_max_query = query % "1=1"
        sql_min_max_query = """
            select min(t.{0}), max(t.{0}) from ({1}) as t
        """.format(filter_field, sub_mix_max_query)
        logging.debug("Running PSQL query: {}".format(sql_min_max_query))
        min_id, max_id = helper.get_results_cursor(
            sql_bind, sql_min_max_query).fetchone()
        return min_id, max_id

    def _get_extract_job_fn_and_params(self, split_range):
        start_pos = end_pos = None
        if split_range:
            start_pos = split_range['start']
            end_pos = split_range['end']
        return dict(worker_callback=helper.export_to_parquet,
                    sql_bind=self._config['sql_bind'],
                    query=self._config['query'],
                    filter_field=self._config.get('filter_field'),
                    output_folder=self._output_folder,
                    start_pos=start_pos,
                    end_pos=end_pos)
