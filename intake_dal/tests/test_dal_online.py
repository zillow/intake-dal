import datetime
import json
from typing import Dict, List
from unittest import mock
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from intake_dal.dal_catalog import DalCatalog
from intake_dal.dal_online import (
    DalOnlineSource,
    deserialize_avro_str_to_pandas,
    serialize_panda_df_to_str,
)


@pytest.fixture
def user_events_df():
    return pd.DataFrame(
        {
            "userid": [100, 101],
            "home_id": [3, 4],
            "action": ["click", "click"],
            "timestamp": [datetime.datetime(2012, 5, 1, 0, 0), datetime.datetime(2012, 5, 2, 0, 0)],
        }
    )


@pytest.fixture
def user_events_with_missing_entries_df():
    return pd.DataFrame(
        {
            "userid": [100, None, 101],
            "home_id": [3, None, 4],
            "action": ["click", None, "click"],
            "timestamp": [datetime.datetime(2012, 5, 1, 0, 0), None, datetime.datetime(2012, 5, 2, 0, 0)],
        }
    )


@pytest.fixture
def user_events_json():
    return [
        {
            "userid": 100,
            "home_id": 3,
            "action": "click",
            "timestamp": {"format": "DATETIME", "time": "2012-05-01 00:00:00.000000"},
        },
        {
            "userid": 101,
            "home_id": 4,
            "action": "click",
            "timestamp": {"format": "DATETIME", "time": "2012-05-02 00:00:00.000000"},
        },
    ]


@pytest.fixture()
def user_events_multi_key_with_some_missing_entries_json():
    return [
        {
            "userid": 100,
            "home_id": 3,
            "action": "click",
            "timestamp": {"format": "DATETIME", "time": "2012-05-01 00:00:00.000000"},
        },
        {},
        {
            "userid": 101,
            "home_id": 4,
            "action": "click",
            "timestamp": {"format": "DATETIME", "time": "2012-05-02 00:00:00.000000"},
        },
    ]


@pytest.fixture
def user_single_event_json():
    return [
        {
            "userid": 1,
            "home_id": 3,
            "action": "click",
            "timestamp": {"format": "DATETIME", "time": "2012-05-01 00:00:00.000000"},
        }
    ]


@pytest.fixture
def user_single_event_df():
    return pd.DataFrame(
        {
            "userid": [1],
            "home_id": [3],
            "action": ["click"],
            "timestamp": [datetime.datetime(2012, 5, 1, 0, 0)]
        }
    )


@mock.patch("intake_dal.dal_online._http_put_avro_data_set")
@mock.patch("intake_dal.dal_online._http_get_avro_data_set")
def test_dal_online_write_read(
        mock_get: MagicMock,
        mock_put: MagicMock,
        serving_cat: DalCatalog,
        user_events_df: pd.DataFrame,
        user_events_json: List[Dict],
):
    canonical_name = "entity.user.user_events"
    avro_str = serialize_panda_df_to_str(
        user_events_df, schema=json.loads(serving_cat.metadata["data_schema"][canonical_name])
    )
    mock_get.return_value = user_events_json
    mock_put.return_value = 200

    serving_cat.entity.user.user_events(storage_mode="serving").write(user_events_df)

    assert_frame_equal(user_events_df, serving_cat.entity.user.user_events(key=100).read(), check_dtype=False)
    mock_get.assert_called()
    mock_put.assert_called()

    assert_frame_equal(
        user_events_df,
        deserialize_avro_str_to_pandas(mock_put.call_args[0][1]["avro_rows"]),
        check_dtype=False,
    )

    assert avro_str != mock_put.call_args[0][1]["avro_rows"]  # not sure why!

    # todo: Why is this not idempotent?
    assert serialize_panda_df_to_str(
        user_events_df, schema=json.loads(serving_cat.metadata["data_schema"][canonical_name])
    ) != serialize_panda_df_to_str(
        user_events_df, schema=json.loads(serving_cat.metadata["data_schema"][canonical_name])
    )


def test_dal_write_parallelism(serving_cat: DalCatalog):
    assert (
            serving_cat["entity.user.user_events"].discover()["metadata"][DalOnlineSource.name][
                "write_parallelism"
            ]
            == 2
    )
    assert (
            serving_cat.entity.user.user_events.discover()["metadata"][DalOnlineSource.name]["write_parallelism"]
            == 2
    )


@mock.patch("intake_dal.dal_online._http_put_avro_data_set")
def test_post_in_chunks(mock_put: MagicMock, serving_cat: DalCatalog, user_events_df: pd.DataFrame):
    mock_put.return_value = 200

    ds: DalOnlineSource = serving_cat["entity.user.user_events"]
    ds.metadata[DalOnlineSource.name]["write_chunk_size"] = 1
    ret = ds.write(user_events_df)

    assert len(mock_put.call_args_list) == 2
    assert len(ret) == 2


@mock.patch("intake_dal.dal_online._http_get_avro_data_set")
def test_dal_online_multi_key_read(
        mock_get: MagicMock,
        serving_cat: DalCatalog,
        user_events_df: pd.DataFrame,
        user_events_json: List[Dict],
):
    mock_get.return_value = user_events_json

    assert_frame_equal(user_events_df, serving_cat.entity.user.user_events(key=[100, 101]).read(), check_dtype=False)
    mock_get.assert_called()
    assert (mock_get.call_args_list[0] == [('https://featurestore.url.net', 'entity.user.user_events', '100,101')])


@mock.patch("intake_dal.dal_online._http_get_avro_data_set")
def test_dal_online_multi_key_read_with_missing_entries(
        mock_get: MagicMock,
        serving_cat: DalCatalog,
        user_events_with_missing_entries_df: pd.DataFrame,
        user_events_multi_key_with_some_missing_entries_json: List[Dict],
):
    mock_get.return_value = user_events_multi_key_with_some_missing_entries_json

    assert_frame_equal(user_events_with_missing_entries_df, serving_cat.entity.user.user_events(key=[1, 2, 3]).read(),
                       check_dtype=False)
    mock_get.assert_called()
    assert (mock_get.call_args_list[0] == [('https://featurestore.url.net', 'entity.user.user_events', '1,2,3')])


@mock.patch("intake_dal.dal_online._http_get_avro_data_set")
def test_dal_online_key_as_string(
        mock_get: MagicMock,
        serving_cat: DalCatalog,
        user_single_event_df: pd.DataFrame,
        user_single_event_json: List[Dict],
):
    mock_get.return_value = user_single_event_json

    assert_frame_equal(user_single_event_df, serving_cat.entity.user.user_events(key="123").read(),
                       check_dtype=False)
    mock_get.assert_called()
    assert len(mock_get.call_args_list) == 1
    assert(mock_get.call_args_list[0] == [('https://featurestore.url.net', 'entity.user.user_events', '123')])
