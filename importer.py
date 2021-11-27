import argparse
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
import signal
from threading import Event, Lock
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import attr
from prometheus_client import start_http_server, Gauge  # type: ignore
from requests_cache import CachedSession
import yaml


def as_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except:
        return None


class InvalidConfigurationError(Exception):
    pass


@attr.s
class Sensor:
    tags = attr.ib(type=dict)
    corrections = attr.ib(type=dict)
    url = attr.ib(type=str)
    hostname = attr.ib(type=Optional[str], default=None)
    map_id = attr.ib(type=Optional[str], default=None)


class Metric(Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"


class Config:
    def __init__(self, path: str) -> None:
        with open(path, "r") as config_file:
            self.config_dict = yaml.load(config_file, Loader=yaml.Loader)

        self.all_tag_keys = set()  # type: Set[str]
        self.all_sensors = list()  # type: List[Sensor]

        if "local" in self.config_dict:
            for item_dict in self.config_dict["local"]:
                if "hostname" not in item_dict:
                    raise InvalidConfigurationError(
                        "Hostname required for all local sensors"
                    )
                hostname = item_dict["hostname"]
                url = "http://{}/json?live=true".format(hostname)
                tags = item_dict.get("tags", dict())
                if not isinstance(tags, dict):
                    raise InvalidConfigurationError(
                        "Tags for {} must be a dictionary".format(hostname)
                    )
                tags["hostname"] = hostname

                corrections = item_dict.get("corrections", dict())
                if not isinstance(corrections, dict):
                    raise InvalidConfigurationError(
                        "Corrections for {} must be a dictionary".format(hostname)
                    )
                unsupported_corrections = set(corrections.keys()) - set(
                    Metric.__members__.keys()
                )
                if unsupported_corrections:
                    raise InvalidConfigurationError(
                        "Unsupported corrections for {}: {}".format(
                            hostname, ", ".join(unsupported_corrections)
                        )
                    )

                self.all_sensors.append(
                    Sensor(
                        hostname=hostname, url=url, tags=tags, corrections=corrections
                    )
                )
                self.all_tag_keys = self.all_tag_keys.union(tags.keys())

        if "map" in self.config_dict:
            for item_dict in self.config_dict["map"]:
                if "id" not in item_dict:
                    raise InvalidConfigurationError("id required for all map sensors")
                id = item_dict["id"]
                url = "https://www.purpleair.com/json?show={}".format(id)
                tags = item_dict.get("tags")
                if not isinstance(tags, dict):
                    raise InvalidConfigurationError(
                        "Tags for {} must be a dictionary".format(id)
                    )
                tags["id"] = id

                corrections = item_dict.get("corrections", dict())
                if not isinstance(corrections, dict):
                    raise InvalidConfigurationError(
                        "Corrections for {} must be a dictionary".format(id)
                    )
                unsupported_corrections = set(corrections.keys()) - set(
                    Metric.__members__.keys()
                )
                if unsupported_corrections:
                    raise InvalidConfigurationError(
                        "Unsupported corrections for {}: {}".format(
                            id, ", ".join(unsupported_corrections)
                        )
                    )

                self.all_sensors.append(
                    Sensor(map_id=id, url=url, tags=tags, corrections=corrections)
                )
                self.all_tag_keys = self.all_tag_keys.union(tags.keys())


class QueryEngine:
    def __init__(
        self, local_query_interval: int, map_query_interval: int, config: Config
    ) -> None:
        self.config = config
        self.local_query_session = CachedSession(expire_after=local_query_interval)
        self.map_query_session = CachedSession(expire_after=map_query_interval)
        self.gauges = {
            Metric.TEMPERATURE: Gauge(
                "purpleair_temperature",
                "Temperature at sensor. May be corrected during import.",
                labelnames=self.config.all_tag_keys,
            ),
            Metric.HUMIDITY: Gauge(
                "purpleair_humidity",
                "Humidity at sensor. May be corrected during import.",
                labelnames=self.config.all_tag_keys,
            ),
        }

    def build_gauges(self) -> None:
        for sensor in self.config.all_sensors:
            for metric, gauge in self.gauges.items():
                gauge.labels(**sensor.tags).set_function(
                    partial(self.get_stat, sensor, metric)
                )

    def get_stat(self, sensor: Sensor, metric: Metric) -> float:
        if sensor.hostname:
            response = self.local_query_session.get(sensor.url)
        else:
            response = self.map_query_session.get(sensor.url)
        results = response.json()["results"]
        assert len(results) == 2

        if metric == Metric.TEMPERATURE:
            return as_float(results[0].get("temp_f")) or as_float(
                results[1].get("temp_f")
            )
        elif metric == Metric.HUMIDITY:
            return as_float(results[0].get("humidity")) or as_float(
                results[1].get("humidity")
            )
        else:
            return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs a webserver for Prometheus scraping and proxies requests to the PurpleAir API."
    )
    parser.add_argument(
        "-c", "--config", help="path to config file (yaml)", default="config.yaml"
    )
    parser.add_argument(
        "-i",
        "--map_query_interval",
        help="minimum interval for map queries (in seconds)",
        default=15,
    )
    parser.add_argument(
        "-l",
        "--local_query_interval",
        help="minimum interval for local queries (in seconds)",
        default=3,
    )
    parser.add_argument(
        "-t", "--timeout", help="timeout for each API call (in seconds)", default=3
    )
    args = parser.parse_args()

    config = Config(args.config)
    query_engine = QueryEngine(
        args.local_query_interval, args.map_query_interval, config
    )
    query_engine.build_gauges()

    exit_event = Event()
    signal.signal(signal.SIGINT, lambda _s, _f: exit_event.set())
    signal.signal(signal.SIGHUP, lambda _s, _f: exit_event.set())

    start_http_server(8000)
    print("Server is running.")
    exit_event.wait()
