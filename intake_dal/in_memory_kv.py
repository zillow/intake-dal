import pkg_resources
from intake import DataSource, Schema
import pandas as pd


class InMemoryKVSource(DataSource):
    container = "dataframe"
    version = pkg_resources.get_distribution("intake-dal").version
    partition_access = False
    name = "in-memory-kvs"

    db = {"first": 1, "second": 2, "third": 3, "fourth": 4}

    def __init__(self, urlpath="", key=None, storage_options=None, metadata=None):
        # store important kwargs
        self._urlpath = urlpath
        self._key = key
        super(InMemoryKVSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        self._dtypes = {"key": "object", "value": "object"}

        return Schema(
            datashape=None,
            dtype=self._dtypes,
            shape=(None, len(self._dtypes)),
            npartitions=1,  # This data is not partitioned, so there is only one partition
            extra_metadata={},
        )

    def _get_partition(self, _) -> pd.DataFrame:
        if self._key:
            return pd.DataFrame(
                [(self._key, self.db[self._key])], columns=["key", "value"], dtype=str
            )
        else:
            return pd.DataFrame(
                list(self.db.items()), columns=["key", "value"], dtype=str
            )

    def _close(self):
        pass