package com.apple.aml.stargate.runners.flink.utils;

import io.prometheus.client.exporter.HTTPServer;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.prometheus.AbstractPrometheusReporter;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.rm.app.ResourceManager.startResourceManager;

@SuppressWarnings("deprecation")
@InstantiateViaFactory(factoryClassName = "com.apple.aml.stargate.runners.flink.utils.FlinkMetricReporterFactory")
public class FlinkMetricReporter extends AbstractPrometheusReporter implements MetricReporter, Scheduled {
    private static final AtomicReference<HTTPServer> METRIC_SERVER = new AtomicReference<>();

    @Override
    public void open(final MetricConfig config) {
        if (METRIC_SERVER.get() == null) {
            synchronized (FlinkMetricReporter.class) {
                //startPrometheusServerThread(METRIC_SERVER); // for future reference
                startResourceManager(); // ideally flink doesn't have any pluggable component into task manager; hence this hack
            }
        }
        super.open(config);
    }

    @Override
    public void close() {
        HTTPServer server = METRIC_SERVER.get();
        if (server == null) {
            return;
        }
        try {
            HTTPServer.class.getMethod("close").invoke(server);
        } catch (Exception e) {

        }
    }

    @Override
    public void report() {

    }
}
