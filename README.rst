A command line interface for sqooping data to Google BigQuery
=============================================================

.. image:: https://travis-ci.org/therako/bqsqoop.svg?branch=master
    :target: https://travis-ci.org/therako/bqsqoop
    :alt: Build Status

.. image:: https://img.shields.io/pypi/v/bq-sqoop.svg
    :target: https://pypi.python.org/pypi/bq-sqoop/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/bq-sqoop.svg
    :target: https://pypi.python.org/pypi/bq-sqoop/
    :alt: Python versions

.. image:: https://img.shields.io/pypi/status/bq-sqoop.svg
    :target: https://pypi.python.org/pypi/bq-sqoop/
    :alt: Package status

.. image:: https://coveralls.io/repos/github/therako/bqsqoop/badge.svg?branch=master
    :target: https://coveralls.io/github/therako/bqsqoop?branch=master
    :alt: Package coverage


This project is to just have a simple cli command to export data from ES, postgres, etc using the CPU's,
It's intended to be used in Data workflow for extracting data out.

Note
----

This is still early in the development and a bit rough around the edges.
Any bug reports, feature suggestions, etc are greatly appreciated. :)


Installation and usage
----------------------

**Installation**
Since this is a Python package available on PyPi you can install it like 
any other Python package.

.. code-block:: shell

    # on modern systems with Python you can install with pip
    $ pip install bq-sqoop
    # on older systems you can install using easy_install
    $ easy_install bq-sqoop

**Usage**
The commands should be mostly self-documenting in how they are defined,
which is made available through the ``help`` command.

.. code-block:: shell

    $ bq-sqoop
    usage: bq-sqoop -h

    arguments:
        -h, --help            show this help message and exit
        -v, --version         Show version and exit.
        -c CONFIG_FILE,       --config_file CONFIG_FILE
                                Toml Config file for the bq-sqoop job.Can be a local
                                file path or a public http link or a GCS fileeg,
                                https://storage.googleapis.com/sample_config.toml or
                                gs://gcs_bucket/sample_config.toml or
                                /tmp/sample_config.toml
        -d, --debug           Debug mode on.


Configuration files
----------------------
You can find an example repository at https://github.com/therako/bqsqoop-examples.git

=========================
Configuration objects
=========================

1. Bigquery_
    .. _Bigquery:

2. Extractor_
    .. _Extractor:

    * Elasticsearch_
        .. _Elasticsearch:

    * SQL_
        .. _SQL:


Bigquery
----------------------

.. code-block:: shell

    [bigquery]
    project_id="destination-google-project-id"
    dataset_name="destination-dataset"
    table_name="destination-table-name"
    gcs_tmp_path="gs://gcs-tmp-bucket/bqsqoop/"


Extractor
----------------------

=========================
Elasticsearch
=========================

.. code-block:: shell

    [extractor.elasticsearch]
    url="localhost:9200,localhost:9201"
    index="source-es-index-name"
    timeout="60s"
    scroll_size=500
    fields=["_all"]


=========================
SQL
=========================

.. code-block:: shell

    [extractor.sql]
    sql_bind="postgresql+psycopg2://username:password@127.0.0.1:5432/database"
    query="select * from table_name"
