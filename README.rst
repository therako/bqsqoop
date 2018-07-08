A command line interface for sqooping data to Google BigQuery
=============================================================

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
      --debug               Debug mode on.
