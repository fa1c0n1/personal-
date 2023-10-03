package com.apple.aml.stargate.beam.sdk.metrics;

import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.MetricUtils;
import com.apple.ihubble.client.stats.HubbleAgent;
import com.typesafe.config.Config;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.IDMS_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.METRIC_DELIMITER;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class HubbleMetricsSink implements MetricsSink, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final String NAMESPACE = "namespace";
    private static final String STEP_NAME = "stepName";
    private transient HubbleAgent agent;
    private String prefix;
    private String appId;
    private String pipelineId;

    public HubbleMetricsSink(final MetricsOptions options) {
    }

    @Override
    public void writeMetrics(final MetricQueryResults results) throws Exception {
        initIfRequired();
        Iterable<MetricResult<Long>> counters = results.getCounters();
        if (counters != null) {
            counters.forEach(counter -> {
                Long value = counter.getAttempted();
                if (value == null) {
                    return;
                }
                MetricKey metricKey = counter.getKey();
                MetricName metricName = metricKey.metricName();
                String sampleName = prefix + metricName.getName().replaceAll(METRIC_DELIMITER, ".");
                agent.captureCount(NAMESPACE, metricName.getNamespace(), sampleName, value);
                agent.captureCount(STEP_NAME, metricKey.stepName(), sampleName, value);
                agent.captureCount(IDMS_APP_ID, appId, sampleName, value);
                agent.captureCount(PIPELINE_ID, pipelineId, sampleName, value);
                agent.captureCount(sampleName, value);
            });
        }
        Iterable<MetricResult<GaugeResult>> gauges = results.getGauges();
        if (gauges != null) {
            gauges.forEach(gauge -> {
                GaugeResult result = gauge.getAttempted();
                if (result == null) {
                    return;
                }
                long value = result.getValue();
                MetricKey metricKey = gauge.getKey();
                MetricName metricName = metricKey.metricName();
                String sampleName = prefix + metricName.getName().replaceAll(METRIC_DELIMITER, ".");
                agent.captureGauge(NAMESPACE, metricName.getNamespace(), sampleName, value);
                agent.captureGauge(STEP_NAME, metricKey.stepName(), sampleName, value);
                agent.captureGauge(IDMS_APP_ID, appId, sampleName, value);
                agent.captureGauge(PIPELINE_ID, pipelineId, sampleName, value);
                agent.captureGauge("epoc", "milliseconds", sampleName, result.getTimestamp().getMillis());
                agent.captureGauge(sampleName, value);
            });
        }
        Iterable<MetricResult<DistributionResult>> dists = results.getDistributions();
        if (dists != null) {
            dists.forEach(dist -> {
                DistributionResult result = dist.getAttempted();
                if (result == null) {
                    return;
                }
                MetricKey metricKey = dist.getKey();
                MetricName metricName = metricKey.metricName();
                String baseSampleName = prefix + metricName.getName().replaceAll(METRIC_DELIMITER, ".");
                Map<String, Long> longs = Map.of("sum", result.getSum(), "count", result.getCount(), "min", result.getMin(), "max", result.getMax());
                for (Map.Entry<String, Long> entry : longs.entrySet()) {
                    String sampleName = baseSampleName + "." + entry.getKey();
                    long value = entry.getValue();
                    agent.captureStat(NAMESPACE, metricName.getNamespace(), sampleName, value);
                    agent.captureStat(STEP_NAME, metricKey.stepName(), sampleName, value);
                    agent.captureStat(IDMS_APP_ID, appId, sampleName, value);
                    agent.captureStat(PIPELINE_ID, pipelineId, sampleName, value);
                    agent.captureStat(sampleName, value);
                }
                Map<String, Double> doubles = Map.of("mean", result.getMean());
                for (Map.Entry<String, Double> entry : doubles.entrySet()) {
                    String sampleName = baseSampleName + "." + entry.getKey();
                    long value = entry.getValue().longValue();
                    agent.captureStat(NAMESPACE, metricName.getNamespace(), sampleName, value);
                    agent.captureStat(STEP_NAME, metricKey.stepName(), sampleName, value);
                    agent.captureStat(IDMS_APP_ID, appId, sampleName, value);
                    agent.captureStat(PIPELINE_ID, pipelineId, sampleName, value);
                    agent.captureStat(sampleName, value);
                }
            });
        }
    }

    private void initIfRequired() {
        if (agent != null) {
            return;
        }
        LOGGER.debug("Initializing Hubble Beam MetricSink..");
        appId = AppConfig.appId() + "";
        pipelineId = pipelineId();
        Config config = AppConfig.config();
        prefix = (config.hasPath("hubble.custom.prefix")) ? config.getString("hubble.custom.prefix") : "";
        agent = MetricUtils.getHubbleAgent();
        LOGGER.debug("Hubble Beam MetricSink initialized successfully");
    }
}
