import argparse
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
import signal
from threading import Event, Lock
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import attr
import aqi
from prometheus_client import start_http_server, Gauge  # type: ignore
from requests_cache import CachedSession
import yaml


class InvalidConfigurationError(Exception):
    pass


@attr.s
class Device:
    tags = attr.ib(type=dict)
    corrections = attr.ib(type=dict)
    url = attr.ib(type=str)
    hostname = attr.ib(type=Optional[str], default=None)
    map_id = attr.ib(type=Optional[str], default=None)


class Metric(Enum):
    AQI = "aqi"
    AQI_2_5 = "aqi pm2.5"
    AQI_10_0 = "aqi pm10.0"
    PM_1_0 = "pm1.0"
    PM_2_5 = "pm2.5"
    PM_10_0 = "pm10.0"
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"


class Config:
    def __init__(self, path: str) -> None:
        with open(path, "r") as config_file:
            self.config_dict = yaml.load(config_file, Loader=yaml.Loader)

        self.all_tag_keys = {"sensor"}
        self.all_devices = list()  # type: List[Device]

        if "local" in self.config_dict:
            for item_dict in self.config_dict["local"]:
                if "hostname" not in item_dict:
                    raise InvalidConfigurationError(
                        "Hostname required for all local devices"
                    )
                hostname = item_dict["hostname"]
                url = "http://{}/json?live=true".format(hostname)
                tags = item_dict.get("tags", dict())
                if not isinstance(tags, dict):
                    raise InvalidConfigurationError(
                        "Tags for {} must be a dictionary".format(hostname)
                    )
                tags["id"] = hostname

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

                self.all_devices.append(
                    Device(
                        hostname=hostname, url=url, tags=tags, corrections=corrections
                    )
                )
                self.all_tag_keys = self.all_tag_keys.union(tags.keys())

        if "map" in self.config_dict:
            for item_dict in self.config_dict["map"]:
                if "id" not in item_dict:
                    raise InvalidConfigurationError("id required for all map devices")
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

                self.all_devices.append(
                    Device(map_id=id, url=url, tags=tags, corrections=corrections)
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
            Metric.AQI: Gauge(
                "purpleair_aqi",
                "AQI at sensor.",
                labelnames=self.config.all_tag_keys,
            ),
            Metric.AQI_2_5: Gauge(
                "purpleair_aqi_pm_2_5",
                "AQI for PM2.5 at sensor.",
                labelnames=self.config.all_tag_keys,
            ),
            Metric.AQI_10_0: Gauge(
                "purpleair_aqi_pm_10_0",
                "AQI for PM10.0 at sensor.",
                labelnames=self.config.all_tag_keys,
            ),
            Metric.PM_1_0: Gauge(
                "purpleair_pm_1_0",
                "PM1.0 at sensor.",
                labelnames=self.config.all_tag_keys,
            ),
            Metric.PM_2_5: Gauge(
                "purpleair_pm_2_5",
                "PM2.5 at sensor.",
                labelnames=self.config.all_tag_keys,
            ),
            Metric.PM_10_0: Gauge(
                "purpleair_pm_10_0",
                "PM10.0 at sensor.",
                labelnames=self.config.all_tag_keys,
            ),
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
        for device in self.config.all_devices:
            for metric, gauge in self.gauges.items():
                for sensor in "a", "b":
                    labels = {
                        k: device.tags.get(k, "") for k in self.config.all_tag_keys
                    }
                    labels["sensor"] = sensor
                    gauge.labels(**labels).set_function(
                        partial(self.get_stat, device, metric, sensor)
                    )

    def get_stat(self, device: Device, metric: Metric, sensor: str) -> float:
        if device.hostname:
            response = self.local_query_session.get(device.url)
            data = response.json()
            if sensor == "a":
                suffix = ""
            else:
                suffix = "_b"
        else:
            response = self.map_query_session.get(device.url)
            results = response.json()["results"]
            assert len(results) == 2
            assert sensor in {"a", "b"}
            if sensor == "a":
                data = results[0]
            else:
                data = results[1]
            suffix = ""

        if metric == Metric.AQI:
            pm2_5 = self._get_stat(data, ["pm2_5_atm", "pm2_5_cf_1"], suffix)
            pm10 = self._get_stat(data, ["pm10_0_atm", "pm10_0_cf_1"], suffix)
            return aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM25, pm2_5),
                    (aqi.POLLUTANT_PM10, pm10),
                ]
            )
        elif metric == Metric.AQI_2_5:
            pm2_5 = self._get_stat(data, ["pm2_5_atm", "pm2_5_cf_1"], suffix)
            return aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM25, pm2_5),
                ]
            )
        elif metric == Metric.AQI_10_0:
            pm10 = self._get_stat(data, ["pm10_0_atm", "pm10_0_cf_1"], suffix)
            return aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM10, pm10),
                ]
            )
        elif metric == Metric.PM_1_0:
            return self._get_stat(data, ["pm1_0_atm", "pm1_0_cf_1"], suffix)
        elif metric == Metric.PM_2_5:
            return self._get_stat(data, ["pm2_5_atm", "pm2_5_cf_1"], suffix)
        elif metric == Metric.PM_10_0:
            return self._get_stat(data, ["pm10_0_atm", "pm10_0_cf_1"], suffix)
        elif metric == Metric.TEMPERATURE:
            return self._get_stat(data, ["temp_f", "current_temp_f"], suffix)
        elif metric == Metric.HUMIDITY:
            return self._get_stat(data, ["humidity", "current_humidity"], suffix)
        else:
            return 0

    def _get_stat(self, data, keys, suffix) -> Optional[float]:
        for key in keys:
            try:
                return float(data[key + suffix])
            except:
                pass
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
