import logging
import traceback
from typing import List

from prometheus_client import Gauge, Counter, Histogram
from stargate.api.stargate_prometheus_service import StargatePrometheusService

METRIC_PREFIX = "pipeline"
counter_dict = {}
histogram_dict = {}
gauge_dict = {}
NODE_NAME = "nodeName"

logger = logging.getLogger(__name__)


def get_updated_metric_name(user_provided_name: str, suffix: str):
    updated_metric_name = f"{METRIC_PREFIX}_{user_provided_name}_{suffix}"
    return updated_metric_name


def get_updated_labels(label_names: List) -> tuple:
    updated_labels = (NODE_NAME,)
    if label_names:
        updated_labels += tuple(label_names)

    return updated_labels


def get_updated_label_values(node_name: str, label_values: List) -> tuple:
    updated_values = (node_name,)
    if label_values:
        updated_values += tuple(label_values)

    return updated_values


class PrometheusServiceImpl(StargatePrometheusService):

    def __init__(self, node_name: str):
        self.node_name = node_name
        logger.info(f"Metric service initialized successfully for node {self.node_name}")

    def new_pipeline_counter(self, name: str, desc: str = None, label_names: List = None):
        counter_name = get_updated_metric_name(user_provided_name=name, suffix="counter")

        if counter_name not in counter_dict:
            counter_dict[counter_name] = Counter(name=counter_name, documentation=desc,
                                                 labelnames=get_updated_labels(label_names))

    def new_pipeline_gauge(self, name: str, desc: str = None, label_names: List = None):
        gauge_name = get_updated_metric_name(user_provided_name=name, suffix="gauge")

        if gauge_name not in gauge_dict:
            gauge_dict[gauge_name] = Gauge(name=gauge_name, documentation=desc,
                                           labelnames=get_updated_labels(label_names))

    def new_pipeline_histogram(self, name: str, desc: str = None, buckets: List = None,
                               label_names: List = None):
        histogram_name = get_updated_metric_name(user_provided_name=name, suffix="histogram")

        if histogram_name not in histogram_dict:
            if buckets is None:
                histogram_dict[histogram_name] = Histogram(name=histogram_name, documentation=desc,
                                                           labelnames=get_updated_labels(label_names))
            else:
                histogram_dict[histogram_name] = Histogram(name=histogram_name, documentation=desc,
                                                           labelnames=get_updated_labels(label_names),
                                                           buckets=tuple(buckets))

    def pipeline_counter(self, name: str, label_values: List = None) -> Counter:
        updated_label_values = get_updated_label_values(self.node_name, label_values)
        updated_counter_name = get_updated_metric_name(user_provided_name=name, suffix="counter")

        try:

            return counter_dict[updated_counter_name].labels(*updated_label_values)
        except Exception as e:
            if updated_counter_name not in counter_dict and not label_values:
                self.new_pipeline_counter(name=name)
                return counter_dict[updated_counter_name].labels(*updated_label_values)

            error_msg = f"Error updating the Counter {name} with label values {label_values} - {traceback.format_exc()}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def pipeline_gauge(self, name: str, label_values: List = None) -> Gauge:
        updated_label_values = get_updated_label_values(self.node_name, label_values)
        updated_gauge_name = get_updated_metric_name(user_provided_name=name, suffix="gauge")

        try:
            return gauge_dict[updated_gauge_name].labels(*updated_label_values)
        except Exception as e:
            if updated_gauge_name not in gauge_dict and not label_values:
                self.new_pipeline_gauge(name=name)
                return gauge_dict[updated_gauge_name].labels(*updated_label_values)

            error_msg = f"Error updating the Gauge {name} with label values {label_values} - {traceback.format_exc()}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def pipeline_histogram(self, name: str, label_values: List = None) -> Histogram:
        updated_label_values = get_updated_label_values(self.node_name, label_values)
        updated_histogram_name = get_updated_metric_name(user_provided_name=name, suffix="histogram")

        try:
            return histogram_dict[updated_histogram_name].labels(*updated_label_values)
        except Exception as e:
            if updated_histogram_name not in histogram_dict and not label_values:
                self.new_pipeline_histogram(name=name)
                return histogram_dict[updated_histogram_name].labels(*updated_label_values)

            error_msg = f"Error updating the Histogram {name} with label values {label_values} - {traceback.format_exc()}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
