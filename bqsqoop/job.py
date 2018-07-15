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

    def _validate_configs(self):
        _extractor = extractor.get_extractor_for(self._config)
        _extractor.validate_config()
        self._validate_bq_configs()

    def _validate_bq_configs(self):
        _bq_config = self._configs.get("bigquery")
        if(len(_bq_config) != 1):
            raise Exception("Missing bigquery configs")
        bq_job.BigqueryParquetLoadJob(_bq_config)
