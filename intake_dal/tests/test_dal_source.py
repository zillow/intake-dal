from urllib.parse import urlparse

from intake_dal.dal_source import DalSource


def test_dal_source_description(cat):
    assert cat.entity.user.user_events.description == "user_events description"


def test_dtype(serving_cat):
    ds = serving_cat.entity.user.user_events(key="a")
    info = ds.discover()

    assert info["dtype"] == {
        "action": "object",
        "home_id": "int32",
        "timestamp": "datetime64",
        "userid": "int64",
    }


def test_avro_schema(serving_cat):
    def validate_avro_schema(schema):
        assert "fields" in schema
        assert schema["name"] == "Root"
        assert schema["type"] == "record"
        assert schema["fields"][0]["name"] == "userid"
        assert schema["fields"][0]["type"] == "long"
        assert schema["fields"][1]["name"] == "home_id"
        assert schema["fields"][1]["type"] == "int"

    ds = serving_cat.entity.user.user_events(key="a")
    info = ds.discover()
    validate_avro_schema(info["metadata"]["avro_schema"])
    validate_avro_schema(ds.avro_schema)


def test_canonical_name(serving_cat):
    ds = serving_cat.entity.user.user_events(key="a")
    assert ds.discover()["metadata"]["canonical_name"] == "entity.user.user_events"


def test_dataset_without_avro_and_engine_arg(serving_cat):
    """Tests an entry in the catalog without an AVRO, and tests engine arg"""
    ds: DalSource = serving_cat.dataset_without_avro(storage_mode="batch")

    # force the DalSource to instantiate the private source object
    ds.discover()

    # ensure that the engine argument is passed along to the DalSource instantiated source object
    assert ds.source._captured_init_kwargs['engine'] == 'fastparquet'
    assert not ds.source._captured_init_kwargs['gather_statistics']


def test_parse_storage_mode_url():
    def validate_url(url: str, expected: str):
        assert DalSource.parse_storage_mode_url(url) == (urlparse(url), expected)

    validate_url("csv://{{ CATALOG_DIR }}/data/user_events.csv", "{{ CATALOG_DIR }}/data/user_events.csv")

    validate_url("dal-online://http://0.0.0.0:9166#zpid", "http://0.0.0.0:9166#zpid")

    validate_url(
        "hive://user_events_dal_catalog2;userid={{userid}}?q1=v1#fragment",
        "user_events_dal_catalog2;userid={{userid}}?q1=v1#fragment",
    )
