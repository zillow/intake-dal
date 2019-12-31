import base64
import io
import time
import urllib.parse
from datetime import datetime
from http import HTTPStatus
from typing import Callable, Dict, List, Tuple
from urllib.parse import ParseResult, urldefrag, urlparse  # noqa: F401

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

    DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(self, urlpath, key=None, storage_options=None, metadata=None):
        """
            Fetches rows from the Online Feature Store (FS).
        Args:
            urlpath: the host path, must include primary_key name as a fragment.
            key: The primary_key value.
            storage_options: optional storage options to send along
            metadata: Used by Intake
        """

        self._urlpath = urlpath
        parse_result = urlparse(urlpath)  # type: ParseResult
        self._url, _ = urldefrag(urlpath)
        self._key_name = parse_result.fragment
        if self._key_name == "":
            raise ValueError(f"key_name expected in URL fragment of {urlpath}")

        self._canonical_name = None  # set in _get_schema()
        self._key_value = key
        super().__init__(metadata=metadata)

    def write(self, df: pd.DataFrame) -> List[Tuple[float, float]]:
        self._get_schema()

        def post_lambda(avro_str: str) -> int:
            return _http_put_avro_data_set(
                self._url,
                {"data_set_name": self._canonical_name, "key_value": self._key_name, "avro_rows": avro_str},
            )

        def get_metadata(key: str, default):
            if DalOnlineSource.name in self.metadata:
                return self.metadata[DalOnlineSource.name].get(key, default)
            else:
                return default

        # get settings with defaults
        write_chunk_size = get_metadata("write_chunk_size", default=1000)
        write_delay_between_chunks_milliseconds = get_metadata(
            "write_delay_between_chunks_milliseconds", default=50
        )
        return _post_in_chunks(
            df, self._avro_schema, post_lambda, write_chunk_size, write_delay_between_chunks_milliseconds
        )

    def _get_partition(self, _) -> pd.DataFrame:
        self._get_schema()
        data = _http_get_avro_data_set(self._url, self._canonical_name, self._key_value)
        for row in data:
            for key, field in row.items():
                if isinstance(field, dict) and "format" in field:
                    row[key] = datetime.strptime(field["time"], self.DATE_TIME_FORMAT)
        return pd.DataFrame(data)

    def _close(self):
        pass

    def _get_schema(self) -> Schema:
        if self._canonical_name is None:
            self._canonical_name = self.metadata["canonical_name"]
            self._avro_schema = self.metadata["avro_schema"]
            self._dtypes = self.metadata["dtypes"]
            self._storage_mode = self.metadata["storage_mode"]

        return Schema(
            datashape=None,
            dtype=self._dtypes,
            shape=(None, len(self._dtypes)),
            npartitions=1,  # This data is not partitioned, so there is only one partition
            extra_metadata={
                "canonical_name": self._canonical_name,
                "storage_mode": self._storage_mode,
                "avro_schema": self._avro_schema,
            },
        )


AVRO_DATA_SETS_PATH = "avro-data-sets"


def _post_in_chunks(
    df: pd.DataFrame,
    avro_schema: Dict,
    post_lambda: Callable[[str], int],
    write_chunk_size: int,
    write_delay_between_chunks_milliseconds: int,
) -> List[Tuple[float, float]]:
    """
    :param df: DataFrame to post
    :param post_lambda: Lambda to pust the avro to and it returns the status code.
    :return: list of durations of how long it took to (serialize to Avro, run post_lambda)
    """
    times = []
    number_of_chunks = np.math.ceil(len(df) / write_chunk_size)
    for i, chunk in enumerate(np.array_split(df, number_of_chunks)):
        if i != 0:
            time.sleep(write_delay_between_chunks_milliseconds / 1000)  # sleep takes seconds

        avro_begin_time = time.time()
        avro_str = serialize_panda_df_to_str(chunk, avro_schema)
        avro_time = time.time() - avro_begin_time

        post_begin_time = time.time()
        post_lambda(avro_str)
        times.append((avro_time, time.time() - post_begin_time))
    return times


def _http_get_avro_data_set(url: str, canonical_name: str, key_value: str) -> List[Dict]:
    response = requests.get(urllib.parse.urljoin(url, f"{AVRO_DATA_SETS_PATH}/{canonical_name}/{key_value}"))
    if response.status_code != HTTPStatus.OK.value:
        raise Exception(f"url={response.url} code={response.status_code}: {response.text}")
    return response.json()["data"]


def _http_put_avro_data_set(url: str, json: Dict) -> int:
    response = requests.put(urllib.parse.urljoin(url, f"{AVRO_DATA_SETS_PATH}/"), json=json)
    if response.status_code != HTTPStatus.OK.value:
        raise Exception(f"url={response.url} code={response.status_code}: {response.text}")
    return response.status_code


def serialize_panda_df_to_str(df: pd.DataFrame, schema: Dict) -> str:
    with io.BytesIO() as bytes_io:
        # else we get: ValueError: NaTType does not support timestamp
        # it's really a pandavro issue, see https://github.com/fastavro/fastavro/issues/313
        # TODO(talebz): Create a Pandavro issue for this!
        df = df.replace({np.nan: None})
        pandavro.to_avro(bytes_io, df, schema=schema)
        bytes_io.seek(0)
        return base64.b64encode(bytes_io.read()).decode("utf-8")


def deserialize_avro_str_to_pandas(avro_str: str, schema: dict = None) -> pd.DataFrame:
    return pandavro.from_avro(io.BytesIO(base64.b64decode(avro_str)), schema)
