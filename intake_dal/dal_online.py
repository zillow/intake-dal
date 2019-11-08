from urllib.parse import urlparse, ParseResult, urldefrag  # noqa: F401
import base64
import io
import json
import urllib.parse
from collections import Iterable
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pandavro
import pkg_resources
import requests
from intake import DataSource, Schema


class DalOnlineSource(DataSource):
    """
    Interfaces with Online FS
    This Intake source is to be used in conjunction with intake-dal
    """
    container = "dataframe"
    partition_access = False
    name = "dal-online"
    version = pkg_resources.get_distribution("intake-dal").version

    def __init__(self, urlpath="", key=None, storage_options=None, metadata=None):
        """
            Fetches from Online FS
        Args:
            urlpath: the host path
            key: key value
            key_name: key name
            storage_options: optional storage options to send along
            metadata:
        """

        self._urlpath = urlpath
        parse_result = urlparse(urlpath)  # type: ParseResult
        (self._url, _) = urldefrag(urlpath)
        self._key_name = parse_result.fragment
        assert self._key_name != '', "key_name expected in URL fragment"
        self._key_value = key
        self._canonical_name = None  # _get_schema() sets this
        super(DalOnlineSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._canonical_name is None:
            self._canonical_name = self.metadata['canonical_name']  # an intake-dal feature
            # TODO(talebz): Getting avro schema should be promoted to Intake
            self._avro_schema = _get_avro(self, self._canonical_name)
            self._schema_dtypes = _avro_to_dtype(self._avro_schema)
            self._dtypes = {k: str(v) for (k, v) in self._schema_dtypes.items()}

        return Schema(
            datashape=None,
            dtype=self._dtypes,
            shape=(None, len(self._dtypes)),
            npartitions=1,  # This data is not partitioned, so there is only one partition
            extra_metadata={
                'canonical_name': self._canonical_name,
                'urlpath': self._urlpath
            },
        )

    def write(self, df: pd.DataFrame):
        self._get_schema()
        avro_str = serialize_panda_df_to_str(df, self._avro_schema)
        _http_put_avro_data_set(
            self._url,
            {'data_set_name': self._canonical_name, 'key_name': self._key_name, 'avro_rows': avro_str}
        )

    def _get_partition(self, _) -> pd.DataFrame:
        self._get_schema()
        response = requests.get(
            urllib.parse.urljoin(self._url, f"avro-data-sets/{self._canonical_name}/{self._key_value}"),
        )

        return deserialize_avro_str_to_pandas(response.json()['avro_rows'])

    def _close(self):
        pass


def serialize_panda_df_to_str(df: pd.DataFrame, schema: Dict) -> str:
    with io.BytesIO() as bytes_io:
        # else we get: ValueError: NaTType does not support timestamp
        # it's really a pandavro issue, see https://github.com/fastavro/fastavro/issues/313
        # TODO(talebz): Create a Pandavro issue for this!
        df = df.replace({np.nan: None})
        pandavro.to_avro(bytes_io, df, schema=schema)
        bytes_io.seek(0)
        return base64.b64encode(bytes_io.read()).decode('utf-8')


def deserialize_avro_str_to_pandas(response_str: str, schema: dict = None) -> pd.DataFrame:
    return pandavro.from_avro(io.BytesIO(base64.b64decode(response_str)), schema)


# TODO(talebz) exponential backoff with retries
def _http_put_avro_data_set(dest_host: str, json: Dict) -> int:
    # HTTP Put!
    response = requests.put(
        urllib.parse.urljoin(dest_host, "avro-data-sets/"),
        json=json
    )

    if response.status_code != 200:
        raise Exception(f"url={response.url} code={response.status_code}: {response.text}")

    return response.status_code


def _get_avro(source: DataSource, canonical_name: str) -> Optional[Dict]:
    data_schema_entry = _get_metadata_schema(source)

    if 'kafka_schema_registry' in data_schema_entry:
        # TODO(talebz): check data_schema_entry for kafka_schema_registry.  If exists then query Kafka Schema Registry
        raise NotImplementedError("kafka_schema_registry integration not yet supported.  "
                                  f"Please put schema as {canonical_name}: > JSON of avro schema")

    if canonical_name in data_schema_entry:
        return json.loads(data_schema_entry[canonical_name])
    else:
        return None


def _get_metadata_schema(source: DataSource) -> Dict:
    if 'data_schema' in source.metadata:
        return source.metadata['data_schema']
    elif source.cat:
        return _get_metadata_schema(source.cat)


# TODO(talebz): ensure this is comprehensive with unit tests!
def _avro_to_dtype(schema: Dict) -> Dict:
    field_schemas = {f['name']: f['type'] for f in schema['fields']}
    avro_type_to_dtype = {
        tuple(sorted(['type', 'long', 'logicalType', 'timestamp-millis'])): np.dtype('datetime64'),
        # tuple(sorted(['type', 'long', 'logicalType', 'timestamp-millis', 'null'])): np.datetime64,
        tuple(sorted(['null', 'int'])): np.dtype('Int32'),
        tuple(sorted(['null', 'long'])): np.dtype('Int32'),
        tuple(sorted(['type', 'int', 'unsigned', 'True'])): np.dtype('UInt32'),
        tuple(sorted(['type', 'long', 'unsigned', 'True'])): np.dtype('Int64'),
        tuple(['long']): np.dtype('int64'),
        tuple(['int']): np.dtype('int32'),
        tuple(['float']): np.dtype('float32'),
        tuple(['double']): np.dtype('float64'),
        tuple(['boolean']): np.dtype('bool'),
        tuple(['string']): np.dtype('object'),
    }

    def to_lookup(avro_type) -> tuple:
        if isinstance(avro_type, str):
            return tuple([avro_type])
        elif isinstance(avro_type, list):
            return tuple(sorted(_flatten(avro_type)))

    ret = {}
    for (k, v) in field_schemas.items():
        lookup = to_lookup(v)
        if lookup in avro_type_to_dtype:
            ret[k] = avro_type_to_dtype[lookup]
        elif 'null' in lookup:
            list_lookup = list(lookup)
            list_lookup.remove('null')
            new_lookup = tuple(list_lookup)
            if new_lookup in avro_type_to_dtype:
                ret[k] = avro_type_to_dtype[new_lookup]
            else:
                raise ValueError(f"{lookup} to pandas type not supported in {schema}")
        else:
            raise ValueError(f"{lookup} to pandas type not supported in {schema}")

    return ret


def _flatten(ls):
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

