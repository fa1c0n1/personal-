package com.apple.aml.stargate.pipeline.parser;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.nodes.StargatePipeline;
import com.apple.aml.stargate.pipeline.pojo.Edge;
import com.apple.aml.stargate.pipeline.pojo.Vertx;
import org.apache.beam.sdk.options.PipelineOptions;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.nio.dot.DOTImporter;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.jgrapht.util.SupplierUtil.createSupplier;

public class DagBuilder <V extends Vertx,E extends Edge>{

    @SuppressWarnings("unchecked")
    public  Map<String, DirectedAcyclicGraph<V, E>> dags(final StargatePipeline definition, final PipelineOptions options, Class vertx, Class edge) {
        Map<String, DirectedAcyclicGraph<V, E>> graphMap = new HashMap<>();
        definition.getJobs().forEach((name, dag) -> {
            DirectedAcyclicGraph<V, E> graph = new DirectedAcyclicGraph<>(createSupplier(vertx), createSupplier(edge), false, false);
            DOTImporter<V, E> importer = new DOTImporter<>();
            importer.addVertexAttributeConsumer((p, a) -> {
                StargateNode node = definition.node(a.getValue());
                if (node == null) throw new InvalidInputException("Every node in DAG should have valid reference in DSL!!", Map.of("dagReference", a.getValue())).wrap();
                p.getFirst().setValue(definition.node(a.getValue()));
            });
            importer.importGraph(graph, new StringReader((String) dag));
            List<V> starters = graph.vertexSet().stream().filter(v -> v.getValue().isActive() && graph.getAncestors(v).isEmpty()).collect(Collectors.toList());
            if (starters.isEmpty()) throw new InvalidInputException("Active starter reader node not found !!").wrap();
            if (starters.stream().noneMatch(v -> v.getValue().isPrimary())) starters.get(0).getValue().setPrimary(true);
            graph.vertexSet().stream().filter(v -> v.getValue().isActive()).forEach(vertex -> {
                if (vertex.runtimeClass() != null) return;
                if (graph.getAncestors(vertex).isEmpty()) {
                    vertex.initRuntimeClass("read", name, options);
                } else if (graph.getDescendants(vertex).isEmpty()) {
                    vertex.initRuntimeClass("write", name, options);
                } else {
                    vertex.initRuntimeClass("transform", name, options);
                }
            });
            graphMap.put(name.toLowerCase(), graph);
        });
        return graphMap;
    }
}
