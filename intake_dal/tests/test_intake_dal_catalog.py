import datetime
import json
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pandas as pd
from pandas.util.testing import assert_frame_equal

from intake_dal.dal_catalog import DalCatalog
from intake_dal.dal_online import (
    deserialize_avro_str_to_pandas,
    serialize_panda_df_to_str,
)


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
    assert cat.entity.user.user_events(storage_mode="in_mem").read().head().shape[0] == 4
    assert (
        cat.entity.user.user_events(storage_mode="local_test", data_path="data").read().head().shape[0] == 2
    )

    df = pd.DataFrame({"key": ["a", "first"], "value": [3, 42]})
    cat.entity.user.user_events(storage_mode="in_mem").write(df)
    assert cat.entity.user.user_events(storage_mode="in_mem", key="a").read().iloc[0].key == "a"
    assert cat.entity.user.user_events(storage_mode="in_mem", key="a").read().iloc[0].value == 3

    # read whole partition
    assert cat.entity.user.user_events(storage_mode="in_mem").read().shape[0] == 5


def test_dal_catalog_set_storage():
    cat = DalCatalog(catalog_path, storage_mode="batch")

    # ensure it's reading the batch storage
    assert cat.entity.user.user_events().read().head().shape[0] == 1

    # test storage storage_mode override:
    assert cat.entity.user.user_events(storage_mode="batch").read().head().shape[0] == 1
    assert cat.entity.user.user_events(storage_mode="local").read().head().shape[0] == 2

    # read by key
    assert cat.entity.user.user_events(storage_mode="in_mem", key="second").read().shape[0] == 1
    assert cat.entity.user.user_events(storage_mode="in_mem", key="second").read().iloc[0].key == "second"
    assert cat.entity.user.user_events(storage_mode="in_mem", key="second").read().iloc[0].value == 2

    assert (
        cat.entity.user.user_events(storage_mode="batch").discover()
        == cat.entity.user.user_events(storage_mode="local").discover()
    )


def test_dal_source_description():
    cat = DalCatalog(catalog_path)  # default is local -> csv plugin
    assert cat.entity.user.user_events.description == "user_events description"


@mock.patch("intake_dal.dal_online.DalOnlineSource._http_put_avro_data_set")
@mock.patch("intake_dal.dal_online.DalOnlineSource._http_get_avro_data_set")
def test_dal_online_write_read(mock_get: MagicMock, mock_put: MagicMock):
    df = pd.DataFrame(
        {
            "userid": [100],
            "home_id": [3],
            "action": ["click"],
            "timestamp": [datetime.datetime(2012, 5, 1, 0, 0)],
        }
    )

    cat = DalCatalog(catalog_path, storage_mode="serving")  # URL path

    canonical_name = "entity.user.user_events"
    avro_str = serialize_panda_df_to_str(df, schema=json.loads(cat.metadata["data_schema"][canonical_name]))
    mock_get.return_value = avro_str
    mock_put.return_value = 200

    cat.entity.user.user_events(storage_mode="serving").write(df)
    assert cat.entity.user.user_events(key=100).read().iloc[0].userid == 100
    assert cat.entity.user.user_events(key=100).read().iloc[0].home_id == 3

    assert_frame_equal(df, cat.entity.user.user_events(key=100).read(), check_dtype=False)

    mock_get.assert_called()
    mock_put.assert_called()

    assert_frame_equal(
        df, deserialize_avro_str_to_pandas(mock_put.call_args[0][0]["avro_rows"]), check_dtype=False
    )

    assert avro_str != mock_put.call_args[0][0]["avro_rows"]  # not sure why!

    # todo: Why is this not idempotent?
    assert serialize_panda_df_to_str(
        df, schema=json.loads(cat.metadata["data_schema"][canonical_name])
    ) != serialize_panda_df_to_str(df, schema=json.loads(cat.metadata["data_schema"][canonical_name]))


def test_dtype():
    cat = DalCatalog(catalog_path, storage_mode="serving")
    ds = cat.entity.user.user_events(key="a")
    info = ds.discover()

    assert info["dtype"] == {
        "action": "object",
        "home_id": "int32",
        "timestamp": "datetime64",
        "userid": "int64",
    }


def test_canonical_name():
    cat = DalCatalog(catalog_path, storage_mode="serving")
    ds = cat.entity.user.user_events(key="a")
    assert ds.discover()["metadata"]["canonical_name"] == "entity.user.user_events"
