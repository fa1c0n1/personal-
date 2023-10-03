package com.apple.aml.stargate.agent.service;

import com.apple.aml.stargate.agent.concurrent.AsyncPromQLService;
import com.apple.aml.stargate.agent.pojo.prom.PromAllMetricsResponse;
import com.apple.aml.stargate.agent.pojo.prom.PromMetadata;
import com.apple.aml.stargate.agent.pojo.prom.PromMetadataResponse;
import com.apple.aml.stargate.common.utils.AppConfig;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Configuration
@EnableScheduling
public class AgentExecutor {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Value("${stargate.observability.agent.executor.fixed.delay.intervalMS}")
    private long scheduleDelayMS;

    @Value("#{'${stargate.observability.agent.prometheus.metricsFilter}'.split(',')}")
    private List<String> metricsFilter;

    @Autowired
    private AsyncPromQLService asyncPromQLService;

    @Autowired
    private PrometheusApiService prometheusApiService;

    @Scheduled(fixedDelayString = "${stargate.observability.agent.executor.fixed.delay.intervalMS}", initialDelayString = "${stargate.observability.agent.executor.initial.delay.intervalMS}")
    public void scheduleFixedDelayTask() {

        if (!AppConfig.config().getBoolean("stargate.observability.agent.enable")) {
            return;
        }

        LOGGER.debug("--------Task Scheduled---------");

        try {
            PromAllMetricsResponse response = prometheusApiService.getAllPromQLMetrics();

            PromMetadataResponse promMetadata = prometheusApiService.getAllPromMetadata();

            Map<String, String> metricsTypeMap = populateMetricsTypeMap(promMetadata);

            Set<String> pipelines = prometheusApiService.getAllUniquePipelines();

            List<String> promFilteredMetrics = new ArrayList<>();

            for (String filter : metricsFilter) {
                promFilteredMetrics.addAll(filterMetrics(response.getData(), filter));
            }

            for (String pipeline : pipelines) {
                try {
                    asyncPromQLService.processPipelineMetadata(promFilteredMetrics, pipeline, metricsTypeMap);
                } catch (Exception e) {
                    LOGGER.error(String.format("Error in processing PipelineId {}", pipeline), e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error In executing Observability agent", e);
        }

    }


    private Map<String, String> populateMetricsTypeMap(PromMetadataResponse response) {

        Map<String, String> map = new HashMap<>();

        if (response != null
                && "success".equals(response.getStatus())
                && response.getData() != null) {
            for (PromMetadata promMetadata : response.getData()) {
                map.put(promMetadata.getMetric(), promMetadata.getType());
            }
        }

        return map;
    }


    private List<String> filterMetrics(List<String> metrics, String filter) {
        List<String> strings = metrics.stream().filter(m -> Pattern.matches(filter, m)).collect(Collectors.toList());
        return strings;
    }
}
