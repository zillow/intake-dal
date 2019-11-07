from urllib.parse import urlparse, ParseResult  # noqa: F401

import pandas as pd

import pkg_resources
from intake import DataSource
from intake.catalog.local import LocalCatalogEntry


class DalSource(DataSource):
    """
    DalSource abstracts a dataset over heterogeneous storage systems.
    DalSource follows the pattern of Intake AliasSource, which in the end
    refers to another driver.

    The ending driver must accept an "urlpath" argument because the URL is
    parsed as such:
     - scheme is the Intake driver name.
     - path becomes the driver "urlpath" argument.

    user_events:
      driver: dal
      args:
        default: 'local'
        storage:
          local: 'csv://{{ CATALOG_DIR }}/data/user_events.csv'
          serving: 'in-memory-kv://foo'
          batch: 'parquet://{{ CATALOG_DIR }}/data/user_events.parquet'
    """

    container = "other"
    name = "dal"
    version = pkg_resources.get_distribution("intake-dal").version

    def __init__(self, storage, default, storage_mode=None, metadata=None, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the file to parse (can be remote)
        reload : bool
            Whether to watch the source file for changes; make False if you want
            an editable Catalog
        """
        super(DalSource, self).__init__(metadata)
        self.storage = storage
        self.default = default
        self.storage_mode = storage_mode
        self.kwargs = kwargs
        self.metadata = metadata
        self.source = None

    def _get_source(self):
        if self.catalog_object is None:
            raise ValueError("DalSource cannot be used outside a catalog")
        if self.source is None:
            self.source = self._instantiate_source()
            self.metadata = self.source.metadata.copy()
            self.container = self.source.container
            self.partition_access = self.source.partition_access
            self.description = self.source.description
            self.datashape = self.source.datashape

    def _instantiate_source(self):
        """ Driving method of this class. """
        mode = self.storage[
            self.storage_mode if self.storage_mode else self.default
        ]

        args = {}
        mode_url = mode
        if isinstance(mode, dict):
            mode_url = mode["url"]
            args = mode.get("args", {})

        # of form: 'csv://{{ CATALOG_DIR }}/data/user_events.csv' where
        #  - scheme is the Intake driver name.
        #  - path becomes the driver "urlpath" argument.
        parse_result = urlparse(mode_url)  # type: ParseResult
        url_path = f"{parse_result.netloc}{parse_result.path}"
        desc = self.catalog_object[self.name].describe()

        if parse_result.scheme == "parquet":
            # https://github.com/dask/dask/issues/5272: Dask parquet metadata w/ ~2k files very slow
            args["gather_statistics"] = False
            args["engine"] = "pyarrow"

        entry = LocalCatalogEntry(
            name=desc["name"],
            description=desc["description"],
            driver=parse_result.scheme,
            args={"urlpath": url_path, **args},
            parameters=self.catalog_object[self.name]._user_parameters,
        )

        return entry.get(metadata=self.metadata, **self.kwargs)

    def discover(self):
        self._get_source()
        return self.source.discover()

    def read(self):
        self._get_source()
        return self.source.read()

    def read_partition(self, i):
        self._get_source()
        return self.source.read_partition(i)

    def read_chunked(self):
        self._get_source()
        return self.source.read_chunked()

    # TODO(talebz): This should also be within Intake but without DataFrame type!
    def write(self, df: pd.DataFrame):
        self._get_source()
        return self.source.write(df)


    def to_dask(self):
        self._get_source()
        return self.source.to_dask()


