import pandas as pd
import pkg_resources
from intake import DataSource, Schema


class InMemoryKVSource(DataSource):
    container = "dataframe"
    version = pkg_resources.get_distribution("intake-dal").version
    partition_access = False
    name = "in-memory-kvs"

    db = pd.DataFrame({"key": ["first", "second", "third", "fourth"], "value": [1, 2, 3, 4]})

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
            return InMemoryKVSource.db[self.db.key == self._key]
        else:
            return InMemoryKVSource.db

    def write(self, df: pd.DataFrame):
        new_db = pd.merge(InMemoryKVSource.db, df, on="key", how="outer")
        new_db["value"] = new_db["value_y"].fillna(new_db["value_x"]).astype("int")

        InMemoryKVSource.db = new_db[["key", "value"]]
        return InMemoryKVSource.db

    def _close(self):
        pass
