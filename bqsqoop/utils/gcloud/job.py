from bqsqoop.utils import typed
from bqsqoop.utils.gcloud import auth, storage, bigquery


class BigqueryParquetLoadJob():
    """Loads local parquet files data into bigquery using google storage

    Init will _errors.append(a error if config is invalid)
    If invalid, errors property will contains the config errors
    and a flag `is_config_valid` is updated accordingly

    Args:
        configs (dict): Job's Bigquery sub-section configs as a dict
    """

    def __init__(self, configs):
        self._project_id = configs.get("project_id")
        self._dataset_name = configs.get("dataset_name")
        self._table_name = configs.get("table_name")
        self._gcs_tmp_path = configs.get("gcs_tmp_path")
        self._service_account_key = configs.get('service_account_key')
        self._write_truncate = configs.get('write_truncate', True)
        self._validate_configs()

    def _validate_configs(self):
        self.errors = {}
        self.is_config_valid = True
        _strings = ["project_id", "dataset_name",
                    "table_name", "gcs_tmp_path"]
        for _str_vars in _strings:
            _res = typed.non_empty_string(getattr(self, "_" + _str_vars))
            if _res:
                self.errors[_str_vars] = _res
        _res = auth.setup_credentials(self._service_account_key)
        if _res:
            self.errors["google_auth"] = _res
        if self.errors:
            self.is_config_valid = False

    def execute(self, files):
        """Executes the job to load parquet files to Bigquery tables

        Args:
            files (list_of_str): List of files to be uploaded to bigquery.
                Should be full file paths

        Returns:
            None if the job is successful, errors if failed
        """
        _gcs_dest_path = storage.parallel_copy_files_to_gcs(
            files, self._gcs_tmp_path, self._project_id,
            use_new_tmp_folder=True)
        bigquery.load_parquet_files(
            _gcs_dest_path + "*.parq",
            self._project_id,
            self._dataset_name,
            self._table_name,
            write_truncate=self._write_truncate
        )
        storage.delete_files_in(_gcs_dest_path, self._project_id)
