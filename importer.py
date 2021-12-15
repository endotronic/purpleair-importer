import argparse
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
import logging
from logging import Logger
import signal
from threading import Event, Lock
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import attr
import aqi  # type: ignore
from prometheus_client import start_http_server  # type: ignore
from prometheus_client.core import GaugeMetricFamily, REGISTRY  # type: ignore
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


class MetricIds(Enum):
    AQI = "aqi"
    AQI_2_5 = "aqi pm2.5"
    AQI_10_0 = "aqi pm10.0"
    PM_1_0 = "pm1.0"
    PM_2_5 = "pm2.5"
    PM_10_0 = "pm10.0"
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"


@attr.s(frozen=True)
class Metric:
    id = attr.ib(type=MetricIds)
    name = attr.ib(type=str)
    help_text = attr.ib(type=str)
    local_json_key = attr.ib(type=Optional[str], default=None)
    remote_json_key = attr.ib(type=Optional[str], default=None)
    multiple_sensors = attr.ib(type=bool, default=False)
    computed = attr.ib(type=bool, default=False)


@attr.s(frozen=True)
class Stat:
    id = attr.ib(type=MetricIds)
    value = attr.ib(type=float)
    labels = attr.ib(type=dict)


METRICS = {
    Metric(
        id=MetricIds.AQI,
        name="purpleair_aqi",
        help_text="AQI at sensor.",
        computed=True,
    ),
    Metric(
        id=MetricIds.AQI_2_5,
        name="purpleair_aqi_pm_2_5",
        help_text="AQI for PM2.5 at sensor.",
        computed=True,
    ),
    Metric(
        id=MetricIds.AQI_10_0,
        name="purpleair_aqi_pm_10_0",
        help_text="AQI for PM10.0 at sensor.",
        computed=True,
    ),
    Metric(
        id=MetricIds.PM_1_0,
        name="purpleair_pm_1_0",
        help_text="PM1.0 at sensor.",
        local_json_key="pm1_0_atm",
        remote_json_key="pm1_0_atm",
        multiple_sensors=True,
    ),
    Metric(
        id=MetricIds.PM_2_5,
        name="purpleair_pm_2_5",
        help_text="PM2.5 at sensor.",
        local_json_key="pm2_5_atm",
        remote_json_key="pm2_5_atm",
        multiple_sensors=True,
    ),
    Metric(
        id=MetricIds.PM_10_0,
        name="purpleair_pm_10_0",
        help_text="PM10.0 at sensor.",
        local_json_key="pm10_0_atm",
        remote_json_key="pm10_0_atm",
        multiple_sensors=True,
    ),
    Metric(
        id=MetricIds.TEMPERATURE,
        name="purpleair_temperature",
        help_text="Temperature at sensor. May be corrected during import.",
        local_json_key="current_temp_f",
        remote_json_key="temp_f",
    ),
    Metric(
        id=MetricIds.HUMIDITY,
        name="purpleair_humidity",
        help_text="Humidity at sensor. May be corrected during import.",
        local_json_key="current_humidity",
        remote_json_key="humidity",
    ),
}


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
                    MetricIds.__members__.keys()
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
                    MetricIds.__members__.keys()
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


class PurpleAirCollector:
    def __init__(
        self,
        local_query_interval: int,
        map_query_interval: int,
        config: Config,
        logger: Logger,
    ) -> None:
        self.config = config
        self.logger = logger
        self.local_query_session = CachedSession(expire_after=local_query_interval)
        self.map_query_session = CachedSession(expire_after=map_query_interval)
        REGISTRY.register(self)

    def describe(self) -> Iterable[GaugeMetricFamily]:
        return []

    def collect(self) -> Iterable[GaugeMetricFamily]:
        stats_by_metric = defaultdict(list)  # type: Dict[MetricIds, List[Stat]]
        for device in self.config.all_devices:
            try:
                if device.hostname:
                    for stat in self.collect_gauges_for_local_device(device):
                        stats_by_metric[stat.id].append(stat)
                else:
                    for stat in self.collect_gauges_for_remote_device(device):
                        stats_by_metric[stat.id].append(stat)
            except:
                self.logger.exception(
                    "Failed to query for device at {}".format(device.url)
                )

        for metric in METRICS:
            gauge = GaugeMetricFamily(
                metric.name,
                metric.help_text,
                labels=self.config.all_tag_keys,
            )
            for stat in stats_by_metric[metric.id]:
                gauge.add_metric(stat.labels.values(), stat.value)
            yield gauge

    def collect_gauges_for_local_device(self, device: Device) -> Iterable[Stat]:
        labels = {k: str(device.tags.get(k, "")) for k in self.config.all_tag_keys}

        response = self.local_query_session.get(device.url)
        data = response.json()

        for metric in METRICS:
            if metric.computed:
                continue

            assert metric.local_json_key, "Metrics are misconfigured"
            if metric.multiple_sensors:
                yield Stat(
                    id=metric.id,
                    value=float(data[metric.local_json_key]),
                    labels=dict(labels, sensor="A"),
                )
                yield Stat(
                    id=metric.id,
                    value=float(data[metric.local_json_key + "_b"]),
                    labels=dict(labels, sensor="B"),
                )

                # See below
                if metric.id == MetricIds.PM_2_5:
                    pm2_5_a = float(data[metric.local_json_key])
                    pm2_5_b = float(data[metric.local_json_key + "_b"])
                if metric.id == MetricIds.PM_10_0:
                    pm10_a = float(data[metric.local_json_key])
                    pm10_b = float(data[metric.local_json_key + "_b"])
            else:
                yield Stat(
                    id=metric.id, value=data[metric.local_json_key], labels=labels
                )

        # The following could be made generic and programatic, but it's more trouble than
        # its worth until there are other calculations besides AQI.
        for sensor, pm2_5, pm10 in [("A", pm2_5_a, pm10_a), ("B", pm2_5_b, pm10_b)]:
            aqi_std = aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM25, pm2_5),
                    (aqi.POLLUTANT_PM10, pm10),
                ]
            )
            aqi_2_5 = aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM25, pm2_5),
                ]
            )
            aqi_10 = aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM10, pm10),
                ]
            )
            for metric_id, val in [
                (MetricIds.AQI, aqi_std),
                (MetricIds.AQI_2_5, aqi_2_5),
                (MetricIds.AQI_10_0, aqi_10),
            ]:
                yield Stat(
                    id=metric_id,
                    value=val,
                    labels=dict(labels, sensor=sensor),
                )

    def collect_gauges_for_remote_device(self, device: Device) -> Iterable[Stat]:
        labels = {k: str(device.tags.get(k, "")) for k in self.config.all_tag_keys}

        response = self.map_query_session.get(device.url)
        results = response.json()["results"]
        assert len(results) == 2

        for metric in METRICS:
            if metric.computed:
                continue

            assert metric.remote_json_key, "Metrics are misconfigured"
            if metric.multiple_sensors:
                yield Stat(
                    id=metric.id,
                    value=float(results[0][metric.remote_json_key]),
                    labels=dict(labels, sensor="A"),
                )
                yield Stat(
                    id=metric.id,
                    value=float(results[1][metric.remote_json_key]),
                    labels=dict(labels, sensor="B"),
                )

                # See below
                if metric.id == MetricIds.PM_2_5:
                    pm2_5_a = float(results[0][metric.remote_json_key])
                    pm2_5_b = float(results[1][metric.remote_json_key])
                if metric.id == MetricIds.PM_10_0:
                    pm10_a = float(results[0][metric.remote_json_key])
                    pm10_b = float(results[1][metric.remote_json_key])
            else:
                yield Stat(
                    id=metric.id,
                    value=results[0][metric.remote_json_key],
                    labels=labels,
                )

        # The following could be made generic and programatic, but it's more trouble than
        # its worth until there are other calculations besides AQI.
        for sensor, pm2_5, pm10 in [("A", pm2_5_a, pm10_a), ("B", pm2_5_b, pm10_b)]:
            aqi_std = aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM25, pm2_5),
                    (aqi.POLLUTANT_PM10, pm10),
                ]
            )
            aqi_2_5 = aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM25, pm2_5),
                ]
            )
            aqi_10 = aqi.to_aqi(
                [
                    (aqi.POLLUTANT_PM10, pm10),
                ]
            )
            for metric_id, val in [
                (MetricIds.AQI, aqi_std),
                (MetricIds.AQI_2_5, aqi_2_5),
                (MetricIds.AQI_10_0, aqi_10),
            ]:
                yield Stat(
                    id=metric_id,
                    value=val,
                    labels=dict(labels, sensor=sensor),
                )


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
    parser.add_argument("--log_level", default="INFO")
    parser.add_argument("--global_log_level", default="INFO")
    args = parser.parse_args()

    log_level = logging.getLevelName(args.log_level)
    global_log_level = logging.getLevelName(args.global_log_level)
    logging.basicConfig(format="[%(levelname)s]: %(message)s", level=global_log_level)
    logger = logging.getLogger("purpleair_importer")
    logger.setLevel(log_level)

    config = Config(args.config)
    _ = PurpleAirCollector(
        args.local_query_interval, args.map_query_interval, config, logger
    )

    exit_event = Event()
    signal.signal(signal.SIGINT, lambda _s, _f: exit_event.set())
    signal.signal(signal.SIGHUP, lambda _s, _f: exit_event.set())

    start_http_server(8000)
    print("Server is running.")
    exit_event.wait()
