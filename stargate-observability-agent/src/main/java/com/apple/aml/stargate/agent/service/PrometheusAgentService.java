package com.apple.aml.stargate.agent.service;

import com.apple.aml.stargate.agent.constants.Constants;
import com.apple.aml.stargate.agent.pojo.prom.PromAllMetricsResponse;
import com.apple.aml.stargate.agent.pojo.prom.PromMetadataResponse;
import com.apple.aml.stargate.agent.pojo.prom.PromQlResponse;
import com.apple.aml.stargate.agent.pojo.prom.Result;
import com.apple.aml.stargate.common.utils.WebUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
public class PrometheusAgentService implements PrometheusApiService {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Value("${stargate.observability.agent.prometheus.url}")
    private String promUrl;

    @Value("${stargate.observability.agent.executor.fixed.delay.intervalMS}")
    private long scheduleDelayMS;

    @Value("${stargate.observability.agent.prometheus.allPipelineMetrics}")
    private String getAllPipelineMetrics;

    @Override
    public PromAllMetricsResponse getAllPromQLMetrics() {

        String query = promUrl + Constants.Prometheus.GET_ALL_METRICS_CONTROLLER;
        try {
            return WebUtils.httpGet(query, null, PromAllMetricsResponse.class, true, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PromMetadataResponse getAllPromMetadata() {

        String query = promUrl + Constants.Prometheus.METADATA_CONTROLLER;
        try {
            return WebUtils.httpGet(query, null, PromMetadataResponse.class, true, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> getAllUniquePipelines() {

        Set<String> pipelines = new HashSet<>();
        try {
            String url = constructRangeQueryURL(getAllPipelineMetrics);
            PromQlResponse response = WebUtils.httpGet(url, null, PromQlResponse.class, true, true);

            if (response != null) {
                for (Result result : response.getData().getResult()) {
                    pipelines.add(result.getMetric().getPipelineId());
                }
            }
            return pipelines;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String constructRangeQueryURL(String query) {
        Instant endInstant = Instant.now();
        long endTime = endInstant.getEpochSecond();
        long startTime = endTime - (scheduleDelayMS / 1000);

        Instant startInstant = Instant.ofEpochSecond(startTime);

        Map<String, String> requestParams = new HashMap<>();
        requestParams.put(Constants.Prometheus.QUERY, query);
        requestParams.put(Constants.Prometheus.QUERY_PARAM_START_DATE, startInstant.toString());
        requestParams.put(Constants.Prometheus.QUERY_PARAM_END_DATE, endInstant.toString());
        requestParams.put(Constants.Prometheus.QUERY_PARAM_STEP, "15s");

        String encodedURL = requestParams.keySet().stream().map(key -> key + "=" + URLEncoder.encode(requestParams.get(key), StandardCharsets.UTF_8)).collect(Collectors.joining("&", promUrl + Constants.Prometheus.QUERY_RANGE_CONTROLLER + "?", ""));

        return encodedURL;
    }

    @Override
    public PromQlResponse getMetricsData(String metricsName, String pipelineId) {

        try {
            String url = constructRangeQueryURL(String.format("%s{pipelineId=\"%s\"}", metricsName, pipelineId));
            PromQlResponse response = WebUtils.httpGet(url, null, PromQlResponse.class, true, true);
            return response;
        } catch (Exception e) {
            LOGGER.error(String.format("Error in getting Metrics %s data for pipelineId %s", metricsName, pipelineId), e);
        }
        return null;
    }
}
