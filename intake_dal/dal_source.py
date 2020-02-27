import json
from typing import Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

import numpy as np
import pandas as pd
import pkg_resources
from intake import DataSource, Schema
from intake.catalog.local import LocalCatalogEntry


class DalSource(DataSource):
    """
    DalSource abstracts a dataset over heterogeneous storage systems.
    DalSource follows the pattern of Intake AliasSource, which in the end
    refers to another driver.

    The ending driver must accept an "urlpath" argument because the URL is
    parsed as such:
     - scheme is the Intake driver name.
     - path becomes the driver "urlpath" argument.

    user_events:
      driver: dal
      args:
        default: 'local'
        storage:
          local: 'csv://{{ CATALOG_DIR }}/data/user_events.csv'
          serving: 'in-memory-kv://foo'
          batch: 'parquet://{{ CATALOG_DIR }}/data/user_events.parquet'
    """

    container = "dataframe"
    name = "dal"
    version = pkg_resources.get_distribution("intake-dal").version

    def __init__(self, storage, default, storage_mode=None, metadata=None, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the file to parse (can be remote)
        reload : bool
            Whether to watch the source file for changes; make False if you want
            an editable Catalog
        """
        super(DalSource, self).__init__(metadata)
        self.storage = storage
        self.default = default
        self.storage_mode = storage_mode
        self.kwargs = kwargs
        self.metadata = metadata
        self.source = None
        self._canonical_name = None  # _get_schema() sets this
        self._avro_schema = None  # _get_schema() sets this
        self._dtypes = None  # _get_schema() sets this

    def _get_source(self):
        if self.catalog_object is None:
            raise ValueError("DalSource cannot be used outside a catalog")
        if self.source is None:
            self._get_schema()
            self.source = self._instantiate_source()
            self.metadata = self.source.metadata.copy()
            self.container = self.source.container
            self.partition_access = self.source.partition_access
            self.description = self.source.description
            self.datashape = self.source.datashape

    def _get_schema(self) -> Schema:
        if self._canonical_name is None:

            self._canonical_name = _get_dal_canonical_name(self)
            # TODO(talebz): Getting avro schema should be promoted to Intake
            avro_schema = _get_avro(self, self._canonical_name)
            if avro_schema:
                self._avro_schema = avro_schema
                self._schema_dtypes = _avro_to_dtype(self._avro_schema)
                self._dtypes = {k: str(v) for (k, v) in self._schema_dtypes.items()}

        return Schema(
            datashape=None,
            dtype=self._dtypes,
            shape=(None, len(self._dtypes) if self._dtypes else None),
            npartitions=1,  # This data is not partitioned, so there is only one partition
            extra_metadata={
                "canonical_name": self._canonical_name,
                "storage_mode": self.storage_mode,
                "avro_schema": self._avro_schema,
            },
        )

    @staticmethod
    def parse_storage_mode_url(mode_url: str) -> Tuple[ParseResult, str]:
        """
        :param mode_url:
            scheme is the Intake driver name.
            examples:
              - "csv://{{ CATALOG_DIR }}/data/user_events.csv"
              - "dal-online://http://0.0.0.0:9166#zpid"
              - "hive://user_events_dal_catalog2;userid={{userid}}"
        :return:
            parse_result and url_path without the scheme, this is sent to the DataSource
            examples:
              - "{{ CATALOG_DIR }}/data/user_events.csv"
              - "http://0.0.0.0:9166#zpid"
              - "user_events_dal_catalog2;userid={{userid}}"
        """
        parse_result: ParseResult = urlparse(mode_url)
        fragment = "" if parse_result.fragment == "" else f"#{parse_result.fragment}"
        query = "" if parse_result.query == "" else f"?{parse_result.query}"

        url_path = f"{parse_result.netloc}{parse_result.path}{parse_result.params}{query}{fragment}"
        return parse_result, url_path

    def _instantiate_source(self):
        """ Driving method of this class. """
        mode = self.storage[self.storage_mode if self.storage_mode else self.default]

        args = {}
        mode_url = mode
        if isinstance(mode, dict):
            mode_url = mode["url"]
            args = mode.get("args", {})

        parse_result, url_path = self.parse_storage_mode_url(mode_url)
        desc = self.catalog_object[self.name].describe()

        if parse_result.scheme == "parquet":
            # https://github.com/dask/dask/issues/5272: Dask parquet metadata w/ ~2k files very slow
            if "gather_statistics" not in args:
                args["gather_statistics"] = False

            if "engine" not in args:
                args["engine"] = "pyarrow"

        entry = LocalCatalogEntry(
            name=desc["name"],
            description=desc["description"],
            driver=parse_result.scheme,
            args={"urlpath": url_path, **args},
            parameters=self.catalog_object[self.name]._user_parameters,
            catalog=self.cat,
        )

        params = {
            "canonical_name": self._canonical_name,
            "storage_mode": self.storage_mode,
            "avro_schema": self._avro_schema,
            "dtypes": self._dtypes,
        }

        source = entry.get(metadata=self.metadata, **self.kwargs)
        # source = entry.get(metadata=self.metadata, **{**self.kwargs, **params})

        source.metadata["url_path"] = url_path
        source.metadata = {**source.metadata, **params}

        return source

    def discover(self):
        self._get_source()
        return self.source.discover()

    def read(self):
        self._get_source()
        return self.source.read()

    def read_partition(self, i):
        self._get_source()
        return self.source.read_partition(i)

    def read_chunked(self):
        self._get_source()
        return self.source.read_chunked()

    # TODO(talebz): This should also be within Intake but without DataFrame type!
    def write(self, df: pd.DataFrame):
        self._get_source()
        return self.source.write(df)

    def to_spark(self):
        self._get_source()
        return self.source.to_spark()

    def to_dask(self):
        self._get_source()
        return self.source.to_dask()

    @property
    def avro_schema(self) -> Dict:
        self._get_source()
        return self._avro_schema

    @property
    def canonical_name(self) -> Dict:
        self._get_source()
        return self._canonical_name


def _get_dal_canonical_name(source: DataSource) -> str:
    def helper(source: DataSource) -> List[str]:
        if source.cat is None:
            return []  # the parent catalog is not part of the canonical name
        elif source.cat:
            return helper(source.cat) + [source.name]

    return ".".join(helper(source))


def _get_avro(source: DataSource, canonical_name: str) -> Optional[Dict]:
    data_schema_entry = _get_metadata_schema(source)
    if data_schema_entry is None:
        return None

    if "kafka_schema_registry" in data_schema_entry:
        # TODO(talebz): check data_schema_entry for kafka_schema_registry.  If exists then query Kafka Schema Registry
        raise NotImplementedError(
            "kafka_schema_registry integration not yet supported.  "
            f"Please put schema as {canonical_name}: > JSON of avro schema"
        )

    if canonical_name in data_schema_entry:
        return json.loads(data_schema_entry[canonical_name])
    else:
        return None


def _get_metadata_schema(source: DataSource) -> Dict:
    if "data_schema" in source.metadata:
        return source.metadata["data_schema"]
    elif source.cat:
        return _get_metadata_schema(source.cat)


# TODO(talebz): ensure this is comprehensive with unit tests!
def _avro_to_dtype(schema: Dict) -> Dict:
    field_schemas = {f["name"]: f["type"] for f in schema["fields"]}
    avro_type_to_dtype = {
        tuple(sorted(["type", "long", "logicalType", "timestamp-millis"])): np.dtype("datetime64"),
        tuple(sorted(["type", "long", "logicalType", "timestamp-micros"])): np.dtype("datetime64"),
        tuple(sorted(["null", "int"])): np.dtype("int32"),
        tuple(sorted(["null", "long"])): np.dtype("int32"),
        tuple(sorted(["type", "int", "unsigned", "True"])): np.dtype("uint32"),
        tuple(sorted(["type", "long", "unsigned", "True"])): np.dtype("int64"),
        tuple(["long"]): np.dtype("int64"),
        tuple(["int"]): np.dtype("int32"),
        tuple(["float"]): np.dtype("float32"),
        tuple(["double"]): np.dtype("float64"),
        tuple(["boolean"]): np.dtype("bool"),
        tuple(["string"]): np.dtype("object"),
    }

    def to_lookup(avro_type: Union[str, list]) -> tuple:
        if isinstance(avro_type, str):
            return tuple([avro_type])
        elif isinstance(avro_type, list):
            return tuple(sorted(_flatten(avro_type)))

    ret = {}
    for (k, v) in field_schemas.items():
        lookup = to_lookup(v)
        if lookup in avro_type_to_dtype:
            ret[k] = avro_type_to_dtype[lookup]
        elif "null" in lookup:
            list_lookup = list(lookup)
            list_lookup.remove("null")
            new_lookup = tuple(list_lookup)
            if new_lookup in avro_type_to_dtype:
                ret[k] = avro_type_to_dtype[new_lookup]
            else:
                raise ValueError(f"{lookup} to pandas type not supported in {schema}")
        else:
            raise ValueError(f"{lookup} to pandas type not supported in {schema}")

    return ret


def _flatten(ls: Iterable) -> Iterable:
    def iter_ls():
        if isinstance(ls, dict):
            return ls.items()
        else:
            return ls

    for i in iter_ls():
        if isinstance(i, Iterable) and not isinstance(i, str):
            for sub_collection in _flatten(i):
                yield sub_collection
        else:
            yield i
