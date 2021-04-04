from pathlib import Path

import yaml
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


@pytest.fixture
def yaml_catalog(catalog_path):
    with open(catalog_path, 'r') as f:
        yaml_catalog = yaml.safe_load(f)
    return yaml_catalog
