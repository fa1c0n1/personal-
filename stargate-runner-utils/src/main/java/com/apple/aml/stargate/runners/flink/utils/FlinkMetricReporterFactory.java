package com.apple.aml.stargate.runners.flink.utils;

import com.apple.aml.stargate.common.utils.AppConfig;
import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;

@SuppressWarnings("deprecation")
@InterceptInstantiationViaReflection(reporterClassName = "com.apple.aml.stargate.runners.flink.utils.FlinkMetricReporter")
public class FlinkMetricReporterFactory implements MetricReporterFactory {
    @Override
    public MetricReporter createMetricReporter(final Properties properties) {
        if (AppConfig.config().getBoolean("prometheus.metrics.flink.enabled") || parseBoolean(properties.getProperty("enabled", "false"))) {
            return new FlinkMetricReporter();
        }
        return new FlinkNoOpMetricReporter();
    }
}
