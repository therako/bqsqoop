import logging

from bqsqoop.utils import log
from bqsqoop import extractor
from bqsqoop.utils.gcloud import job as bq_job


class Job():
    """A bq-sqoop's Job definition

    Given a set of configs will create a Sqoop job to extract data from
    source and load it to Google Bigquery table.

    example:

    Args:
        configs (dict): Configs for the job execution
            More examples on config definition at example/configs
        debug_mode: A Optional boolean representing log level,
            Default is False
    """

    def __init__(self, configs, debug_mode=False):
        log.setup(debug_mode)
        self._configs = configs
        self._validate_configs()
        logging.debug("Loading the job with configs: {}".format(self._configs))

    def _validate_configs(self):
        self._extractor = extractor.get_extractor_for(self._configs)
        self._extractor.validate_config()
        self._validate_bq_configs()

    def _validate_bq_configs(self):
        _bq_config = self._configs.get("bigquery")
        if not _bq_config:
            raise Exception("Missing bigquery configs")
        self._bq_job = bq_job.BigqueryParquetLoadJob(_bq_config)
        if not self._bq_job.is_config_valid:
            logging.error("Invalid Bigquery configs: {}".format(
                self._bq_job.errors))
            raise Exception("Invalid Bigquery configs")

    def execute(self):
        """Executes the job of extracting data to  Bigquery
        """
        _extracted_files = self._extractor.extract_to_parquet()
        self._bq_job.execute(_extracted_files)
