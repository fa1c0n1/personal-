package org.apache.beam.sdk.metrics;

import com.apple.aml.stargate.beam.sdk.metrics.PrometheusMetricsContainer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class MetricsEnvironment {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final AtomicReference<PrometheusMetricsContainer> GLOBAL_CONTAINER = new AtomicReference<>(new PrometheusMetricsContainer());

    public static boolean isMetricsSupported() {
        return true;
    }

    public static void setMetricsSupported(final boolean supported) {
        // do nothing
    }

    public static MetricsContainer setCurrentContainer(final MetricsContainer container) {
        return GLOBAL_CONTAINER.get();
    }

    public static MetricsContainer setProcessWideContainer(final MetricsContainer container) {
        return GLOBAL_CONTAINER.get();
    }

    public static Closeable scopedMetricsContainer(final MetricsContainer container) {
        return GLOBAL_CONTAINER.get();
    }

    public static MetricsContainer getCurrentContainer() {
        return GLOBAL_CONTAINER.get();
    }

    public static MetricsContainer getProcessWideContainer() {
        return GLOBAL_CONTAINER.get();
    }
}
