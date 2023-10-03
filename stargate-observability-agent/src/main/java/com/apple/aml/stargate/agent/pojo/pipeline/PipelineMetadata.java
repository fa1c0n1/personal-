package com.apple.aml.stargate.agent.pojo.pipeline;

import lombok.Data;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class PipelineMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private Date firstEventSeenOn;

    private AgentMetadataLatestRun latestRun;

    private Set<AgentMetadataMetrics> metrics = new HashSet<>();

    private Set<AgentMetadataPipelineNode> nodes = new HashSet<>();

    public boolean updatePipelineMetadata(PipelineMetadata pipelineMetadata) {
        if (this.firstEventSeenOn == null || (pipelineMetadata.firstEventSeenOn != null && this.firstEventSeenOn.after(pipelineMetadata.firstEventSeenOn))) {
            this.firstEventSeenOn = pipelineMetadata.firstEventSeenOn;
        }

        if (this.latestRun == null) {
            this.latestRun = pipelineMetadata.latestRun;
        } else {
            this.latestRun.updateLatestRun(pipelineMetadata.latestRun);
        }

        Map<String, AgentMetadataMetrics> metricsMap = convertToMap(pipelineMetadata.metrics);
        for (AgentMetadataMetrics metric : metrics) {
            if (metricsMap.containsKey(metric.id)) {
                metricsMap.get(metric.getId()).updateMetrics(metric);
            } else {
                metricsMap.put(metric.id, metric);
            }
        }
        metrics = new HashSet<>(metricsMap.values());

        if (nodes == null) {
            nodes = pipelineMetadata.getNodes();
        } else {
            Map<String, AgentMetadataPipelineNode> map = convertToMap(nodes);

            for (AgentMetadataPipelineNode node : pipelineMetadata.nodes) {
                if (map.containsKey(node.getNodeName())) {
                    map.get(node.getNodeName()).updateNode(node);
                } else {
                    map.put(node.getNodeName(), node);
                }
            }

            nodes = new HashSet<>(map.values());
        }
        return true;
    }

    public Map<String, AgentMetadataMetrics> convertToMap(Collection<AgentMetadataMetrics> metrics) {
        Map<String, AgentMetadataMetrics> map = metrics.stream()
                .collect(Collectors.toMap(AgentMetadataMetrics::getId, Function.identity()));

        return map;
    }

    public Map<String, AgentMetadataPipelineNode> convertToMap(Set<AgentMetadataPipelineNode> nodes) {
        Map<String, AgentMetadataPipelineNode> map = nodes.stream()
                .collect(Collectors.toMap(AgentMetadataPipelineNode::getNodeName, Function.identity()));
        return map;
    }

}
