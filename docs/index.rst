.. intake-dal documentation master file
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Intake DAL (data access layer) plugin
==================================================

This `Intake <https://intake.readthedocs.io/en/latest/quickstart.html>`_ plugin helps
abstract a dataset over heterogeneous storage systems.
It also provides a helper method to specialize a
`hierarchical catalog <https://github.com/zillow/intake-nested-yaml-catalog/>`_
to a default storage system.

.. code-block:: yaml

    user_events:
      driver: dal
      args:
        default: 'local'
        storage:
          local: 'csv://{{ CATALOG_DIR }}/data/user_events.csv'
          serving: 'in-memory-kv://foo'
          batch: 'parquet://{{ CATALOG_DIR }}/data/user_events.parquet'


.. code-block:: python

  # Specialize the catalog dal default storge mode datasources
  # to be "serving".
  cat = DalCatalog(path, storage_mode="serving")

  # reads from the serving storage system
  # using the in-memory-kv Intake plugin
  df = cat.user_events.read()

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
