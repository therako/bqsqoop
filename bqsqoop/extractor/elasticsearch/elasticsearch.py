import logging
from .helper import ESHelper
from bqsqoop.utils import async_worker
from bqsqoop.extractor import Extractor
from bqsqoop.utils.errors import MissingConfigError


class ElasticSearchExtractor(Extractor):
    """Extractor data from Elasticsearch index
    """
    def __init__(self):
        self._no_of_workers = self._config.get('no_of_workers', 1)
        self._timeout = self._config.get('timeout', '60s')
        self._scroll_size = self._config.get('scroll_size', 1000)
        self._fields = self._config.get('fields', ['_all'])
        self._output_folder = self._config.get('output_folder', '.')
        super().__init__()

    def validate_config(self):
        """Validates required configs for Elasticsearch extraction
        """
        if 'url' not in self._config:
            raise MissingConfigError('url', 'Elasticsearch')
        if 'index' not in self._config:
            raise MissingConfigError('index', 'Elasticsearch')
        return None

    def extract_to_parquet(self):
        _async_worker = async_worker.AsyncWorker(
            self._no_of_workers)
        for i in range(self._no_of_workers):
            _async_worker.send_data_to_worker(
                worker_id=i,
                **self._get_extract_job_fn_and_params()
            )
        logging.debug('Waiting for Extractor job results...')
        return _async_worker.get_job_results()

    def _get_extract_job_fn_and_params(self):
        search_args = dict(
            index=self._config['index'],
            scroll=self._timeout,
            size=self._scroll_size,
            body={
                'query': {
                    'match_all': {}
                }
            }
        )
        if '_all' not in self._fields:
            search_args['_source_include'] = ','.join(
                self._fields)
        _fields = ESHelper.get_fields(
            self._config['url'], self._config['index'])
        return dict(worker_callback=ESHelper.scroll_and_extract_data,
                    total_worker_count=self._no_of_workers,
                    es_hosts=self._config['url'],
                    es_timeout=self._timeout,
                    output_folder=self._output_folder,
                    search_args=search_args,
                    fields=_fields)
