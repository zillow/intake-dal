import functools

import pkg_resources
import yaml
from intake import Catalog
from intake.utils import yaml_load
from intake_nested_yaml_catalog.nested_yaml_catalog import (
    NestedYAMLFileCatalog,
)

from intake_dal.dal_source import DalSource


class DalCatalog(NestedYAMLFileCatalog):
    """
    DalCatalog combines the functionality of a nested hierarchical catalog
    along with a the "dal" DataSource.
    """

    name = "dal_cat"
    version = pkg_resources.get_distribution("intake-dal").version

    def __init__(self, path, storage_mode=None, autoreload=True, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the file to parse (can be remote)
        reload : bool
            Whether to watch the source file for changes; make False if you want
            an editable Catalog
        storage_mode: str
            The dal default storage mode override for this instantiation of the
            catalog.

        Example catalog:
          sources:
            user_events:
              driver: dal
              args:
                default: 'local'
                storage:
                  local: 'csv://{{ CATALOG_DIR }}/data/user_events.csv'
                  serving: 'in-memory-kv://foo'
                  batch: 'parquet://{{ CATALOG_DIR }}/data/user_events.parquet'

        Following overrides the default from 'local' to 'serving'.
        >>> cat = DalCatalog(path, storage_mode="serving")
        >>> df = cat.user_events.read()
        """
        self.storage_mode = storage_mode
        super(DalCatalog, self).__init__(path, autoreload, **kwargs)

    def __getitem__(self, key):
        # TODO(Taleb Zeghmi): Remove once https://github.com/zillow/intake-nested-yaml-catalog/issues/6 is resolved
        if len(key.split(".")) > 1:
            return self._construct_dataset(key, self)
        else:
            ret = super().__getitem__(key)
            return ret

    def parse(self, text):
        data = yaml_load(text)

        # modify sources default storage mode
        self._set_dal_default_storage_mode(data)
        transformed_text = yaml.dump(data, default_flow_style=False)

        # Reuse default NestedYAMLFileCatalog YAML parser
        # parse() does the heavy lifting of populating the catalog
        super().parse(transformed_text)

    def _set_dal_default_storage_mode(self, data):
        """
        Traverses the catalog to set all default dal source
        storage modes to self.storage_mode
        """
        if self.storage_mode:
            for k, v in data.items():
                if isinstance(v, dict) and v.get("driver", None) == "dal":
                    v["args"]["default"] = self.storage_mode
                elif isinstance(v, dict):
                    self._set_dal_default_storage_mode(v)

    @staticmethod
    def _construct_dataset(canonical_name: str, catalog: Catalog) -> DalSource:
        catalog_entity = functools.reduce(lambda acc, x: acc[x], canonical_name.split("."), catalog)
        return catalog_entity
