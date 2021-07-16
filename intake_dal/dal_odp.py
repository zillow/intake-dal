import re
import urllib.parse
from collections import Iterable
from datetime import datetime
from http import HTTPStatus
from typing import Dict, List

import pandas as pd
import pkg_resources
import requests

from intake_dal.dal_online import DalOnlineSource


class DalOdpSource(DalOnlineSource):

    container = "dataframe"
    partition_access = False
    name = "dal-odp"
    version = pkg_resources.get_distribution("intake-dal").version

    def __init__(self, urlpath, key=None, version="latest", storage_options=None, metadata=None):
        self._version = version
        super().__init__(urlpath=urlpath, key=key, storage_options=storage_options, metadata=metadata)

    def read(self, timeout: float = 10, key=None, version=None):
        if key:
            self._key_value = key
        if version:
            self._version = version
        return self._get_partition(timeout)

    def _get_partition(self, timeout: float) -> pd.DataFrame:
        def http_get_argument():
            if isinstance(self._key_value, Iterable) and not isinstance(self._key_value, str):
                return ",".join(map(str, self._key_value))
            else:
                return self._key_value

        self._get_schema()

        data = _http_get_avro_data_set(self._url, self._key_name, http_get_argument(), self._version, timeout)
        for row in data:
            for key, field in row.items():
                if isinstance(field, dict) and "format" in field:
                    row[key] = datetime.strptime(field["time"], self.DATE_TIME_FORMAT)
        return pd.DataFrame(data)


def _http_get_avro_data_set(
    url: str, odp_name: str, key_value: str, version: str, timeout: float
) -> List[Dict]:
    m = re.search(r"https*:\/\/odp-api.*\.(.*)-k8s", url)
    if m:
        env = m.group(1)
    else:
        raise Exception(f"ODP url {url} does not seem to contain an environment")

    headers = {
        "dev": {"accept": "*/*", "x-consumer-username": "zon-test-odp-api-dev"},
        "stage": {"accept": "*/*", "x-consumer-username": "zon-test-stage"},
        # TODO
        "prod": {"accept": "*/*", "x-consumer-username": "zon-test-odp-api-prod"},
    }
    response = requests.get(
        urllib.parse.urljoin(url, f"getRecord/{odp_name}/{key_value}/{version}?output=AvroToJson"),
        headers=headers[env],
        timeout=timeout,
    )
    if response.status_code != HTTPStatus.OK.value:
        raise Exception(f"url={response.url} code={response.status_code}: {response.text}")
    return [response.json()["payload"]]
