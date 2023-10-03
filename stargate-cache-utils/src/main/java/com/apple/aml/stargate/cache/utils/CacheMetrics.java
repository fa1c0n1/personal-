package com.apple.aml.stargate.cache.utils;

import io.prometheus.client.Histogram;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CAUSE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.METHOD_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.PROVIDER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SCOPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.STATUS;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;

public final class CacheMetrics {
    public static final Histogram CREATE_LATENCIES_HISTOGRAM = bucketBuilder("cache_create_latencies_ms", Histogram.build().help("Cache create latencies in ms").labelNames(SOURCE, PROVIDER, CACHE_TYPE, METHOD_NAME, SCOPE, STATUS, CAUSE)).register();
    public static final Histogram READ_LATENCIES_HISTOGRAM = bucketBuilder("cache_read_latencies_ms", Histogram.build().help("Cache read latencies in ms").labelNames(SOURCE, PROVIDER, CACHE_TYPE, METHOD_NAME, SCOPE, STATUS, CAUSE)).register();
    public static final Histogram UPDATE_LATENCIES_HISTOGRAM = bucketBuilder("cache_update_latencies_ms", Histogram.build().help("Cache update latencies in ms").labelNames(SOURCE, PROVIDER, CACHE_TYPE, METHOD_NAME, SCOPE, STATUS, CAUSE)).register();
    public static final Histogram DELETE_LATENCIES_HISTOGRAM = bucketBuilder("cache_delete_latencies_ms", Histogram.build().help("Cache delete latencies in ms").labelNames(SOURCE, PROVIDER, CACHE_TYPE, METHOD_NAME, SCOPE, STATUS, CAUSE)).register();
}
