package com.apple.aml.stargate.agent.concurrent;

import com.apple.aml.stargate.agent.data.LocalCache;
import com.apple.aml.stargate.agent.pojo.pipeline.AgentCacheMetadata;
import com.apple.aml.stargate.agent.pojo.pipeline.AgentMetadataMetrics;
import com.apple.aml.stargate.agent.pojo.pipeline.AgentMetadataPipelineNode;
import com.apple.aml.stargate.agent.pojo.pipeline.PipelineMetadata;
import com.apple.aml.stargate.agent.pojo.pipeline.AgentMetadataSchema;
import com.apple.aml.stargate.agent.pojo.prom.Metric;
import com.apple.aml.stargate.agent.pojo.prom.PromQlResponse;
import com.apple.aml.stargate.agent.pojo.prom.Result;
import com.apple.aml.stargate.agent.service.MetadataService;
import com.apple.aml.stargate.agent.service.PrometheusApiService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
public class AsyncPromQLService {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    private LocalCache localCache;

    @Autowired
    private PrometheusApiService prometheusApiService;

    @Autowired
    private MetadataService metadataService;

    @Async("threadPoolExecutor")
    public void processPipelineMetadata(List<String> metrics, String pipelineId, Map<String, String> metricsTypeMap) throws JsonProcessingException {

        boolean isChange = false;

        AgentCacheMetadata cacheMetadata = localCache.getPipelineMetadata(pipelineId);
        try {
            for (String metric : metrics) {

                PromQlResponse promQlResponse = prometheusApiService.getMetricsData(metric, pipelineId);

                if (promQlResponse != null && promQlResponse.getData() != null && promQlResponse.getData().getResult() != null) {

                    isChange = updatePipelineMetadata(pipelineId, promQlResponse.getData().getResult(), metricsTypeMap, cacheMetadata) || isChange;
                }
            }

            LOGGER.debug("Pipeline {} Data Collection completed ", pipelineId);

            cacheMetadata = localCache.getPipelineMetadata(pipelineId);

            cacheMetadata.setSyncedWithAdminApp(cacheMetadata.isSyncedWithAdminApp() && !isChange);

            if (!cacheMetadata.isSyncedWithAdminApp()) {
                LOGGER.debug("Updating pipelineId to Admin App {}", pipelineId);

                metadataService.updatePipelineMetadata(pipelineId, cacheMetadata);
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Error in processing Pipeline metadata for pipelineId %s", pipelineId), e);
        }

    }

    private boolean updatePipelineMetadata(String pipelineId, List<Result> results, Map<String, String> metricsTypeMap, AgentCacheMetadata cacheMetadata) {

        boolean isChanged = false;

        if (cacheMetadata == null) {
            cacheMetadata = new AgentCacheMetadata();
            cacheMetadata.setPipelineMetadata(new PipelineMetadata());
        }

        results.addAll(composeRootNodes(results));

        Map<String, AgentMetadataMetrics> metricsMap = cacheMetadata.getPipelineMetadata().convertToMap(cacheMetadata.getPipelineMetadata().getMetrics());
        Map<String, AgentMetadataPipelineNode> nodeMap = cacheMetadata.getPipelineMetadata().convertToMap(cacheMetadata.getPipelineMetadata().getNodes());
        for (Result result : results) {
            String metricsName = result.getMetric().get__name__();
            String nodeName = result.getMetric().getNodeName();

            AgentMetadataMetrics metrics = metricsMap.get(metricsName);

            if (metricsMap.containsKey(metricsName)) {
                isChanged = metrics.updateMetrics(getPipelineMetrics(result.getMetric(), metricsTypeMap)) || isChanged;
            } else {
                metricsMap.put(metricsName, getPipelineMetrics(result.getMetric(), metricsTypeMap));
                isChanged = true;
            }

            if (nodeName != null) {
                if (nodeMap.containsKey(nodeName)) {
                    isChanged = nodeMap.get(nodeName).updateNode(getPipelineNode(result.getMetric())) || isChanged;
                } else {
                    nodeMap.put(nodeName, getPipelineNode(result.getMetric()));
                    isChanged = true;
                }
            }

        }
        cacheMetadata.getPipelineMetadata().setNodes(new HashSet<>(nodeMap.values()));
        cacheMetadata.getPipelineMetadata().setMetrics(new HashSet<>(metricsMap.values()));


        return isChanged;
    }

    private List<Result> composeRootNodes(List<Result> results) {
        List<Result> composedRootResult = new ArrayList<>();

        for (Result result : results) {
            if (result.getMetric() != null
                    && result.getMetric().getNodeName() != null
                    && result.getMetric().getNodeName().contains(":")) {
                Result rootResult = new Result();
                Metric rootMetrics = new Metric(result.getMetric());
                rootResult.setMetric(rootMetrics);
                rootResult.setValues(result.getValues());
                String rootNodeName = rootResult.getMetric().getNodeName();
                rootNodeName = StringUtils.split(rootNodeName, ":")[0];
                rootResult.getMetric().setNodeName(rootNodeName);
                composedRootResult.add(rootResult);
            }
        }

        return composedRootResult;
    }

    public AgentMetadataMetrics getPipelineMetrics(Metric metric, Map<String, String> metricsTypeMap) {
        AgentMetadataMetrics metrics = new AgentMetadataMetrics();

        metrics.setId(metric.get__name__());
        if (metric.getNodeName() != null) {
            metrics.getApplicableNodes().add(metric.getNodeName());
        }

        if (metric.getSchemaId() != null) {
            metrics.getApplicableSchemaIds().add(metric.getSchemaId());
        }

        metrics.setFirstEventSeenOn(new Date(Instant.now().toEpochMilli()));

        metrics.setType(metricsTypeMap.get(metric.get__name__()));

        return metrics;
    }

    private AgentMetadataPipelineNode getPipelineNode(Metric metric) {

        if (metric.getNodeName() == null) {
            return null;
        }

        AgentMetadataPipelineNode node = new AgentMetadataPipelineNode();

        node.setNodeName(metric.getNodeName());
        if (metric.getStage() != null) {
            node.getStages().add(metric.getStage());
        }

        if (metric.getSchemaId() != null && !"unknown".equals(metric.getSchemaId()) && "elements_out".equals(metric.getStage())) {
            node.getSchemaIds().add(metric.getSchemaId());
        }

        node.setFirstEventSeenOn(new Date(Instant.now().toEpochMilli()));
        AgentMetadataSchema schema = getPipelineSchema(metric);
        if (schema != null) {
            node.getSchemas().add(schema);
        }

        if ("stargate_counter_total".equals(metric.get__name__())
                && "elements_out".equals(metric.getStage())
                && metric.getAttributeName() != null
                && "sourceSchemaId".equals(metric.getAttributeName())) {
            node.getSchemaMapping().put(metric.getAttributeValue(), metric.getSchemaId());
        }
        return node;
    }

    private AgentMetadataSchema getPipelineSchema(Metric metric) {

        if (metric.getSchemaId() == null || "unknown".equals(metric.getSchemaId())) {
            return null;
        }

        AgentMetadataSchema schema = new AgentMetadataSchema();
        schema.setSchemaId(metric.getSchemaId());
        schema.setFirstEventSeenOn(new Date(Instant.now().toEpochMilli()));

        return schema;
    }
}
