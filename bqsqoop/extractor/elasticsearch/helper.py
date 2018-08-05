import os
import uuid
import elasticsearch
import pandas as pd

from bqsqoop.utils import parquet_util
from bqsqoop.utils import pandas_util
from bqsqoop.utils.progressbar_util import ProgressBar


class ESHelper():
    @classmethod
    def get_fields(self, es_hosts, index):
        _es = self._get_es_client(es_hosts)
        _mappings = _es.indices.get_mapping(
            index=index
        )
        _fields = {}
        for index, value in _mappings.items():
            index_mappings = value['mappings']
            for _, index_type_value in index_mappings.items():
                _properties = index_type_value['properties']
                _keys = list(_properties.keys())
                for _key in _keys:
                    if "type" in _properties[_key]:
                        _fields[_key] = _properties[_key]["type"]
                break
        return _fields

    @classmethod
    def scroll_and_extract_data(self, worker_id, total_worker_count, es_hosts,
                                es_timeout, search_args, fields,
                                output_folder, progress_bar=True,
                                datetime_format="%Y-%m-%dT%H:%M:%S",
                                type_cast={}):
        _es = self._get_es_client(es_hosts)
        search_args = self._add_slice_if_needed(
            total_worker_count, search_args, worker_id)
        _output_file = self._output_file_for(
            output_folder, search_args['index'])
        _page = _es.search(**search_args)
        _data, _sid = self._get_data_from_es_page(_page)
        _total_hits = self._get_total_hits(_page)
        _parquetUtil = parquet_util.ParquetUtil(_output_file)
        _pbar = ProgressBar(
            total_length=_total_hits, position=worker_id, enabled=progress_bar)
        if _data:
            schema = _parquetUtil.build_pyarrow_schema(fields)
            self._write_data(_data, fields, _parquetUtil,
                             _pbar, datetime_format, type_cast, schema)
            while True:
                _page = _es.scroll(scroll_id=_sid, scroll=es_timeout)
                _data, _sid = self._get_data_from_es_page(_page)
                if _data:
                    self._write_data(_data, fields, _parquetUtil, _pbar,
                                     datetime_format, type_cast, schema)
                else:
                    break
        _parquetUtil.close()
        return _output_file

    @classmethod
    def _write_data(self, data, fields, parquetUtil, pbar, datetime_format,
                    type_cast, schema):
        df = pd.DataFrame.from_dict(data)
        datetime_fields = [k for k, v in fields.items() if v == "date"]
        df = pandas_util.PandasUtil.fix_dataframe(
            df,
            type_castings=type_cast,
            datetime_format=datetime_format, datetime_fields=datetime_fields,
            column_schema=fields)
        parquetUtil.append_df_to_parquet(df, schema=schema)
        pbar.move_progress(len(data))

    @classmethod
    def _output_file_for(self, output_folder, index):
        return os.path.join(output_folder, '{}_{}.parq'.format(
            index, str(uuid.uuid4())[:8]))

    @classmethod
    def _get_es_client(self, es_hosts):
        return elasticsearch.Elasticsearch(es_hosts)

    @classmethod
    def _add_slice_if_needed(self, total_worker_count, search_args, worker_id):
        if total_worker_count > 1:
            # Add ES scroll slicing to handle parallel scrolling of the data
            search_args['body']['slice'] = {
                'id': worker_id,
                'max': total_worker_count
            }
        return search_args

    @classmethod
    def _get_data_from_es_page(self, _page):
        if 'hits' in _page and 'hits' in _page['hits']:
            _data = _page['hits']['hits']
            if _data:
                _rows = [_datumn['_source'] for _datumn in _data]
                _sid = _page['_scroll_id']
                return _rows, _sid
        return None, None

    @classmethod
    def _get_total_hits(self, _page):
        if 'hits' in _page and 'total' in _page['hits']:
            return _page['hits']['total']
        return 0
