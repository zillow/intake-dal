from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from intake_dal.dal_catalog import DalCatalog


@pytest.fixture
def odp_cat(catalog_path: str):
    return DalCatalog(str(Path(__file__).resolve().parent.joinpath(Path("odpCatalog.yaml"))))


@pytest.fixture
def zcr_json():
    return [
        {
            "regionidcounty": 1390,
            "zpid": 100,
            "parcelid": 74032563,
            "stacklow": 294595.44,
            "stack": 358578.03,
            "stackhigh": 423164.88,
            "stackfsd": 0.22526099,
        }
    ]


@pytest.fixture
def zcr_df(zcr_json):
    return pd.DataFrame(zcr_json)


@mock.patch("intake_dal.dal_odp._http_get_avro_data_set")
def test_dal_odp_read(mock_get: MagicMock, odp_cat: DalCatalog, zcr_json, zcr_df):
    mock_get.return_value = zcr_json

    assert_frame_equal(
        zcr_df,
        odp_cat.entity.property.zcr.oob_filtered(storage_mode="serving", key=100).read(),
        check_dtype=False,
    )
    mock_get.assert_called()
