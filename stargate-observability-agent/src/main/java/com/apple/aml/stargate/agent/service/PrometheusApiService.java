package com.apple.aml.stargate.agent.service;

import com.apple.aml.stargate.agent.pojo.prom.PromAllMetricsResponse;
import com.apple.aml.stargate.agent.pojo.prom.PromMetadataResponse;
import com.apple.aml.stargate.agent.pojo.prom.PromQlResponse;

import java.util.Set;

public interface PrometheusApiService {

    PromAllMetricsResponse getAllPromQLMetrics();

    PromMetadataResponse getAllPromMetadata();

    Set<String> getAllUniquePipelines();

    PromQlResponse getMetricsData(String metricsName, String pipelineId);

}
