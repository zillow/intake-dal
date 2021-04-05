.. image:: https://travis-ci.org/zillow/intake-dal.svg?branch=master
    :target: https://travis-ci.org/zillow/intake-dal

.. image:: https://coveralls.io/repos/github/zillow/intake-dal/badge.svg?branch=master
    :target: https://coveralls.io/github/zillow/intake-dal?branch=master

.. image:: https://readthedocs.org/projects/intake-dal/badge/?version=latest
    :target: https://intake-dal.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


Welcome to Intake DAL (data access layer) plugin
==================================================
This `Intake <https://intake.readthedocs.io/en/latest/quickstart.html>`_ plugin helps
abstract a dataset over disparate storage systems (eg: bulk, streaming, serving, ...).
It also provides an easy way to specialize a
`hierarchical catalog <https://github.com/zillow/intake-nested-yaml-catalog/>`_
to a default DAL storage system.

Install package:

.. code-block::
    >> pip3 install intake-dal -i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple/

    // Install a package with specific version
    >> pip3 install intake-dal==0.1.10 -i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple/

Sample Catalog source entry:

.. code-block:: yaml

    user_events:
      driver: dal
      args:
        default: 'local'
        storage:
          local: 'csv://{{ CATALOG_DIR }}/data/user_events.csv'
          serving: 'in-memory-kv://foo'
          batch: 'parquet://{{ CATALOG_DIR }}/data/user_events.parquet'

Example code using sample catalog:

.. code-block:: python

  # Specialize the catalog dal default storge mode datasources
  # to be "serving".
  cat = DalCatalog(path, storage_mode="serving")

  # reads from the serving storage system
  # using the in-memory-kv Intake plugin
  df = cat.user_events.read()


.. code-block:: python

  # Initialize a catalog from a YAML string only`
  cat = DalCatalog(None, storage_mode="batch", yaml_catalog=yaml_string)

  # reads from the batch storage system
  df = cat.entity.user.user_events().read()
