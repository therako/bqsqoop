import re
import logging

from google.cloud import bigquery


def load_parquet_files(gcs_path, gcs_project, dataset_name, table_name,
                       write_truncate=True):
    """Creates/Updates BQ table with parquet files from GCS bucket path

    Args:
        gcs_path (str): Should be a path in GCS. Starts with gs://
        gcs_project (str): Google project id for Biguery's
        dataset_name (str): Bigquery dataset name, must exist in project
        table_name (str): Bigquery table name
        write_truncate (bool): flag to define either to Override or Append
            data to the table. Default is True.

    Returns:
        None
    """
    _validate_gcs_path(gcs_path)
    _load_files_from_gcs(gcs_path, gcs_project, dataset_name,
                         table_name, "PARQUET", write_truncate)


def _load_files_from_gcs(gcs_path, gcs_project, dataset_name,
                         table_name, file_format, write_truncate):
    logging.debug("Loading data from {0} to bq table {1}.{2}.{3}\n".format(
        gcs_path, gcs_project, dataset_name, table_name))
    _client = bigquery.Client(project=gcs_project)
    _job_config = bigquery.LoadJobConfig()
    _job_config.source_format = file_format
    _job_config.write_disposition = 'WRITE_TRUNCATE' if \
        write_truncate else 'WRITE_APPEND'
    target_table = _client.dataset(dataset_name).table(table_name)
    job = _client.load_table_from_uri(gcs_path, target_table,
                                      job_config=_job_config)
    logging.debug("Waiting for load job to finish...")
    job.result()
    logging.info("Table {} created".format(table_name))


def _validate_gcs_path(gcs_path):
    gcs_bucket_path_re_pattern = r'gs://.+'
    _match_obj = re.match(gcs_bucket_path_re_pattern, gcs_path)
    if not _match_obj:
        raise Exception("Invalid GCS path")
