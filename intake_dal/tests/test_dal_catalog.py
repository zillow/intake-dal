import pandas as pd

from intake_dal.dal_catalog import DalCatalog


"""
Test setup:
    - batch storage mode driver is parquet and '{{ CATALOG_DIR }}/data/user_events.parquet' has ONLY 1 row
    - local storage mode driver is csv and '{{ CATALOG_DIR }}/data/user_events.csv' has TWO rows
"""


def test_dal_catalog_default_storage_parameter(cat):
    # cat default is local -> csv plugin
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


def test_dal_catalog_set_storage(catalog_path):
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

    batch_info = cat.entity.user.user_events(storage_mode="batch").discover()["metadata"]
    local_info = cat.entity.user.user_events(storage_mode="local").discover()["metadata"]

    assert batch_info["avro_schema"] == local_info["avro_schema"]
    assert batch_info["dtypes"] == local_info["dtypes"]
    assert batch_info["canonical_name"] == local_info["canonical_name"]
    assert batch_info["storage_mode"] == "batch"
    assert local_info["storage_mode"] == "local"


def test_dal_online_key_value_on_read(serving_cat: DalCatalog):
    serving_cat.entity.user.user_events(storage_mode="in_mem", key="first").read()
    pass


def test_construct_dataset(cat):
    def validate_dataset(ds):
        assert ds.name == "user_events"
        assert ds.canonical_name == "entity.user.user_events"
        assert len(ds.read()) > 0
        assert ds.cat.cat.cat == cat

    validate_dataset(cat["entity.user.user_events"])
    validate_dataset(cat.entity["user.user_events"])
    validate_dataset(cat.entity.user["user_events"])
