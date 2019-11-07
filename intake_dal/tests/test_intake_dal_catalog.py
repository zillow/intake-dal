from pathlib import Path

from intake_dal.dal_catalog import DalCatalog

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

    import pandas as pd
    df = pd.DataFrame({'key': ['a', 'first'], 'value': [3, 42]})
    cat.entity.user.user_events(storage_mode="serving").write(df)
    assert cat.entity.user.user_events(storage_mode="serving", key="a").read().iloc[0].key == "a"
    assert cat.entity.user.user_events(storage_mode="serving", key="a").read().iloc[0].value == 3


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
    assert cat.entity.user.user_events(storage_mode="serving", key="second").read().iloc[0].value == 2

    assert cat.entity.user.user_events(storage_mode="batch").discover() == \
           cat.entity.user.user_events(storage_mode="local").discover()


def test_dal_source_description():
    cat = DalCatalog(catalog_path)  # default is local -> csv plugin
    assert cat.entity.user.user_events.description == "user_events description"

