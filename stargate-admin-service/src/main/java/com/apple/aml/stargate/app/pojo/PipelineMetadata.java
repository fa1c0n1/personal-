package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class PipelineMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    @BsonProperty(value = "metrics")
    public Set<Metrics> metrics;
    @BsonProperty(value = "firstEventSeenOn")
    private Date firstEventSeenOn;
    @BsonProperty(value = "latestRun")
    private LatestRun latestRun;
    @BsonProperty(value = "nodes")
    private Set<Node> nodes;

    public boolean updatePipelineMetadata(PipelineMetadata pipelineMetadata) {
        if (this.firstEventSeenOn == null || (pipelineMetadata.firstEventSeenOn != null && this.firstEventSeenOn.after(pipelineMetadata.firstEventSeenOn))) {
            this.firstEventSeenOn = pipelineMetadata.firstEventSeenOn;
        }

        if (this.latestRun == null) {
            this.latestRun = pipelineMetadata.latestRun;
        } else {
            this.latestRun.updateLatestRun(pipelineMetadata.latestRun);
        }

        Map<String, Metrics> metricsMap = convertToMap(pipelineMetadata.metrics);
        for (Metrics metric : metrics) {
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
            Map<String, Node> map = convertToMap(nodes);

            for (Node node : pipelineMetadata.nodes) {
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

    private Map<String, Metrics> convertToMap(Collection<Metrics> metrics) {
        Map<String, Metrics> map = metrics.stream()
                .collect(Collectors.toMap(Metrics::getId, Function.identity()));

        return map;
    }

    private Map<String, Node> convertToMap(Set<Node> nodes) {
        Map<String, Node> map = nodes.stream()
                .collect(Collectors.toMap(Node::getNodeName, Function.identity()));
        return map;
    }

}
