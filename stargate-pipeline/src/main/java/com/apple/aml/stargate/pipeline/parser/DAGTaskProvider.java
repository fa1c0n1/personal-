package com.apple.aml.stargate.pipeline.parser;

import com.apple.aml.stargate.common.nodes.StargatePipeline;
import com.apple.aml.stargate.pipeline.pojo.BeamEdge;
import com.apple.aml.stargate.pipeline.pojo.BeamVertx;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;
import lombok.SneakyThrows;
import org.apache.beam.repackaged.direct_java.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;
import org.slf4j.Logger;

import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.parser.PipelineParser.initializeBeamPipeline;
import static com.apple.aml.stargate.pipeline.parser.PipelineParser.startMetricCollectionThread;

public class DAGTaskProvider implements TaskProvider<String, State> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final Map<String, DirectedAcyclicGraph<BeamVertx, BeamEdge>> dagMap;
    private final PipelineOptions options;
    private final StargatePipeline stargatePipeline;

    public DAGTaskProvider(final Map<String, DirectedAcyclicGraph<BeamVertx, BeamEdge>> dagMap, final PipelineOptions options, final StargatePipeline stargatePipeline) {
        this.dagMap = dagMap;
        this.options = options;
        this.stargatePipeline = stargatePipeline;
    }

    @SneakyThrows
    public Task<String, State> provideTask(final String id) {
        DirectedAcyclicGraph<BeamVertx, BeamEdge> graph = dagMap.get(id);
        Pipeline pipeline = initializeBeamPipeline(options, stargatePipeline, id, graph);
        return new Task<>() {
            public State execute() {
                long startTime = System.currentTimeMillis();
                try {
                    LOGGER.info(String.format("\n\nExecuting DAG [%s] now.. ", id), Map.of("dagName", id, "epoc", Instant.now()));
                    try {
                        StringWriter stringWriter = new StringWriter();
                        DOTExporter<BeamVertx, BeamEdge> exporter = new DOTExporter<>(v -> v.getValue().getName().replaceAll("~", EMPTY_STRING));
                        exporter.setVertexAttributeProvider(v -> Map.of("name", DefaultAttribute.createAttribute(v.getValue().getName()), "type", DefaultAttribute.createAttribute(v.getValue().getType()), "label", DefaultAttribute.createAttribute(String.format("%s(%s)", v.getValue().getName(), v.getValue().getType()))));
                        exporter.exportGraph(graph, stringWriter);
                        LOGGER.debug("\n\n----------- INPUT DOT GRAPH START -----------\n\n" + stringWriter + "\n\n----------- INPUT DOT GRAPH END -----------\n");
                        System.out.println("----------- INPUT DOT GRAPH START -----------");
                        System.out.println(stringWriter);
                        System.out.println("----------- INPUT DOT GRAPH END -----------");
                        String dot = PipelineDotRenderer.toDotString(pipeline);
                        LOGGER.debug("\n\n----------- BEAM DOT GRAPH START -----------\n\n" + dot + "\n\n----------- BEAM DOT GRAPH END -----------\n");
                        System.out.println("----------- BEAM DOT GRAPH START -----------");
                        System.out.println(dot);
                        System.out.println("----------- BEAM DOT GRAPH END -----------");
                    } catch (Exception e) {
                        LOGGER.debug("Could not generate DOT Graph!! DAG is still intact. Will proceed further!!", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())));
                        LOGGER.trace("Could not generate DOT Graph!! DAG is still intact. Will proceed further!!", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                    }
                    startTime = System.currentTimeMillis();
                    PipelineResult result = pipeline.run();
                    startMetricCollectionThread(pipeline, result);
                    State state = result.waitUntilFinish();
                    LOGGER.info("DAG executed successfully !!", Map.of("dagName", id, "state", state, "timeTaken", System.currentTimeMillis() - startTime));
                    return state;
                } catch (Exception e) {
                    LOGGER.warn("Error in executing DAG !!", Map.of("dagName", id, "timeTaken", System.currentTimeMillis() - startTime, "errorStackTrace", ExceptionUtils.getStackTrace(e)), e);
                    return State.FAILED;
                }
            }
        };
    }
}
