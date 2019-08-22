from pathlib import Path

import pandas as pd
from intake import registry, DataSource, Schema

from intake_dal.dal_catalog import DalCatalog


class InMemoryKVSource(DataSource):
    container = "dataframe"
    version = "0.0.1"
    partition_access = False
    name = "in_memory_kv_store"

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


registry['in-memory-kv'] = InMemoryKVSource
catalog_path = str(Path(__file__).resolve().parent.joinpath(Path("catalog.yaml")))


"""
Test setup:
    - batch storage mode driver is parquet and '{{ CATALOG_DIR }}/data/user_events.parquet' has ONLY 1 row
    - local storage mode driver is csv and '{{ CATALOG_DIR }}/data/user_events.csv' has TWO rows
"""


def test_dal_catalog_default_storage_parameter():
    cat = DalCatalog(catalog_path)  # default is local -> csv plugin
    assert cat.entity.user.user_events().read().head().shape[0] == 2
    assert cat.entity.user.user_events(storage_mode="local").read().head().shape[0] == 2
    assert cat.entity.user.user_events(storage_mode="batch").read().head().shape[0] == 1
    assert cat.entity.user.user_events(storage_mode="serving").read().head().shape[0] == 4
    assert cat.entity.user.user_events(storage_mode="local_test", data_path="data").read().head().shape[0] == 2


def test_dal_catalog_set_storage():
    cat = DalCatalog(catalog_path, storage_mode="batch")

    # ensure it's reading the batch storage
    assert cat.entity.user.user_events().read().head().shape[0] == 1

    # test storage storage_mode override:
    assert cat.entity.user.user_events(storage_mode="batch").read().head().shape[0] == 1
    assert cat.entity.user.user_events(storage_mode="local").read().head().shape[0] == 2

    # read whole partition
    assert cat.entity.user.user_events(storage_mode="serving").read().shape[0] == 4

    # read by key
    assert cat.entity.user.user_events(storage_mode="serving", key="second").read().shape[0] == 1
    assert cat.entity.user.user_events(storage_mode="serving", key="second").read().iloc[0].key == "second"
    assert cat.entity.user.user_events(storage_mode="serving", key="second").read().iloc[0].value == "2"

    assert cat.entity.user.user_events(storage_mode="batch").discover() == \
           cat.entity.user.user_events(storage_mode="local").discover()


def test_dal_source_description():
    cat = DalCatalog(catalog_path)  # default is local -> csv plugin
    assert cat.entity.user.user_events.description == "user_events description"

