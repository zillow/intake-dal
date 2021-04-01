from pathlib import Path

import pytest

from intake_dal.dal_catalog import DalCatalog


@pytest.fixture
def catalog_path():
    return str(Path(__file__).resolve().parent.joinpath(Path("catalog.yaml")))


@pytest.fixture
def serving_cat(catalog_path: str):
    return DalCatalog(catalog_path, storage_mode="serving")


@pytest.fixture
def cat(catalog_path: str):
    return DalCatalog(catalog_path)
