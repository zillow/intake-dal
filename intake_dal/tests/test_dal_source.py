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
