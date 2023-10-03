package com.apple.aml.stargate.pipeline.boot;

import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.common.nodes.StargatePipeline;
import com.apple.aml.stargate.pipeline.parser.DAGTaskProvider;
import com.apple.aml.stargate.pipeline.parser.DagBuilder;
import com.apple.aml.stargate.pipeline.parser.FlinkDAGTaskProvider;
import com.apple.aml.stargate.pipeline.parser.PipelineParser;
import com.apple.aml.stargate.pipeline.pojo.BeamEdge;
import com.apple.aml.stargate.pipeline.pojo.BeamVertx;
import com.apple.aml.stargate.pipeline.pojo.FlinkEdge;
import com.apple.aml.stargate.pipeline.pojo.FlinkVertx;
import com.apple.aml.stargate.pipeline.pojo.FlowEdge;
import com.apple.aml.stargate.pipeline.pojo.FlowVertex;
import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.Duration;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.support.ThreadPoolUtil;
import com.github.dexecutor.core.task.ExecutionResults;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.nio.dot.DOTImporter;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.A3Constants.APP_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APP_ON_EXECUTION_ACTION;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.VERSION_INFO;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.ClassUtils.As;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.initializeMetrics;
import static com.apple.aml.stargate.pipeline.parser.PipelineParser.DEFAULT_DAG_NAME;
import static com.apple.aml.stargate.pipeline.parser.PipelineParser.digraph;
import static com.apple.aml.stargate.pipeline.parser.PipelineParser.fetch;
import static com.apple.aml.stargate.rm.app.ResourceManager.startResourceManager;
import static com.apple.aml.stargate.rm.service.RMCoreService.PIPELINE_STATUS;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static org.jgrapht.util.SupplierUtil.createSupplier;

public class Executor implements Closeable {
    private static final String PIPELINE_ID_ARG = "--pipelineId=";
    private static final String PIPELINE_TOKEN_ARG = "--pipelineToken=";
    private static final String PIPELINE_A3_TOKEN_ARG = "--a3Token=";
    private static final String PIPELINE_APP_ID_ARG = "--appId=";
    private static final String PIPELINE_SYSTEM_PROPERTIES_FILE = "--systemPropertiesFile=";
    private static String STARGATE_PIPELINE_TOKEN = "STARGATE_PIPELINE_TOKEN";
    private static String STARGATE_A3_TOKEN = "STARGATE_A3_TOKEN";
    private static String STARGATE_PIPELINE_TOKEN_PREFIX = "SG_TOKEN_";

    public static void main(final String[] args) throws Exception {
        overrideAppIdAndApplyDefaults(args);
        final Logger logger = logger(MethodHandles.lookup().lookupClass()); // ensure this is method level variable and not class level to honor --appId setting
        if (logger.isTraceEnabled()) {
            for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
                logger.trace("Pipeline system env {} == {}", entry.getKey(), entry.getValue());
            }
            for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
                logger.trace("Pipeline system prop {} == {}", entry.getKey(), entry.getValue());
            }
        }
        logger.info("Stargate Version", As(VERSION_INFO, Map.class));
        String[] arguments = args;
        logger.debug("Pipeline invoked with {}", (Object[]) arguments);
        Optional<String> pipelineIdOptional = Arrays.stream(args).filter(arg -> arg.contains(PIPELINE_ID_ARG)).findFirst();
        String pipelineId;
        if (pipelineIdOptional.isPresent()) {
            pipelineId = pipelineIdOptional.get().split("=")[1].trim();
        } else {
            pipelineId = pipelineId();
            arguments = Arrays.copyOf(arguments, arguments.length + 1);
            arguments[args.length] = PIPELINE_ID_ARG + "\"" + pipelineId + "\"";
        }
        if (isBlank(pipelineId)) throw new IllegalArgumentException("Missing pipelineId!!");
        System.setProperty(STARGATE_PIPELINE_ID, pipelineId);
        if (Arrays.stream(args).noneMatch(arg -> arg != null && !arg.isEmpty() && arg.contains(PIPELINE_TOKEN_ARG))) {
            String pipelineToken = env(STARGATE_PIPELINE_TOKEN_PREFIX + pipelineId.replace('-', '_'), null);
            if (isBlank(pipelineToken)) {
                pipelineToken = env(STARGATE_PIPELINE_TOKEN, env("APP_PIPELINE_TOKEN", null));
            }
            if (!isBlank(pipelineToken)) {
                pipelineToken = pipelineToken.replaceAll("'", "").replaceAll("\"", "").trim();
                if (!pipelineToken.isBlank()) {
                    arguments = Arrays.copyOf(arguments, arguments.length + 1);
                    arguments[args.length] = PIPELINE_TOKEN_ARG + "\"" + pipelineToken + "\"";
                }
            }
        }
        if (Arrays.stream(args).noneMatch(arg -> arg != null && !arg.isEmpty() && arg.contains(PIPELINE_A3_TOKEN_ARG))) {
            String a3Token = env(STARGATE_A3_TOKEN, env("APP_A3_TOKEN", null));
            if (!isBlank(a3Token)) {
                a3Token = a3Token.replaceAll("'", "").replaceAll("\"", "").trim();
                if (!a3Token.isBlank()) {
                    arguments = Arrays.copyOf(arguments, arguments.length + 1);
                    arguments[args.length] = PIPELINE_A3_TOKEN_ARG + "\"" + a3Token + "\"";
                }
            }
        }
        initializeMetrics();
        logger.debug("Preparing Pipeline now : {}", Instant.now());
        Pair<PipelineOptions, StargatePipeline> pair = fetch(arguments);
        PipelineOptions options = pair.getLeft();
        StargateOptions stargateOptions = options.as(StargateOptions.class);

        StargatePipeline stargatePipeline = pair.getRight();
        Map<String, DirectedAcyclicGraph<BeamVertx, BeamEdge>> dagMap = null;
        Map<String, DirectedAcyclicGraph<FlinkVertx, FlinkEdge>> flinkDagMap = null;
        ExecutorService executorService = null;
        DexecutorConfig<String, State> config =null;
        DefaultDexecutor<String, State> executor ;
        if(stargateOptions.getPipelineRunner().equalsIgnoreCase("nativeflink")){
            DagBuilder<FlinkVertx,FlinkEdge> flinkDagBuilder= new DagBuilder<>();
            flinkDagMap = flinkDagBuilder.dags(stargatePipeline,options,FlinkVertx.class,FlinkEdge.class);
            if (flinkDagMap.isEmpty()) throw new IllegalArgumentException("Missing DAG!!");
            executorService = Executors.newFixedThreadPool(Math.max(ThreadPoolUtil.poolSize(0), flinkDagMap.keySet().size()));
            config = new DexecutorConfig<>(executorService, new FlinkDAGTaskProvider(flinkDagMap, options.as(StargateOptions.class), stargatePipeline));
            executor = new DefaultDexecutor<>(config);
            if (flinkDagMap.keySet().size() == 1 || stargatePipeline.getJobGraph() == null || stargatePipeline.getJobGraph().toString().trim().isBlank()) {
                flinkDagMap.keySet().forEach(dagName -> executor.addIndependent(dagName));
            } else {
                constructFlowGraphForFlink(stargatePipeline, flinkDagMap, executor, logger);
            }
            logger.debug("Running Flink Pipeline now : {}", Instant.now());
            startResourceManager();
            PipelineParser.loadPipelineSchemas(stargatePipeline);
        }else{
            DagBuilder<BeamVertx,BeamEdge> beamDagBuilder= new DagBuilder<>();
            dagMap = beamDagBuilder.dags(stargatePipeline,options,BeamVertx.class,BeamEdge.class);
            if (dagMap.isEmpty()) throw new IllegalArgumentException("Missing DAG!!");
            executorService = Executors.newFixedThreadPool(Math.max(ThreadPoolUtil.poolSize(0), dagMap.keySet().size()));
            config = new DexecutorConfig<>(executorService, new DAGTaskProvider(dagMap, options, stargatePipeline));
            executor = new DefaultDexecutor<>(config);
            if (dagMap.keySet().size() == 1 || stargatePipeline.getJobGraph() == null || stargatePipeline.getJobGraph().toString().trim().isBlank()) {
                dagMap.keySet().forEach(dagName -> executor.addIndependent(dagName));
            } else {
                constructFlowGraph(stargatePipeline, dagMap, executor, logger);
            }
            logger.debug("Running Pipeline now : {}", Instant.now());
            startResourceManager();
            PipelineParser.loadPipelineSchemas(stargatePipeline);
        }
        ExecutionResults<String, State> results = executor.execute(ExecutionConfig.TERMINATING.scheduledRetrying(stargateOptions.getMaxAttempts(), new Duration(10, TimeUnit.SECONDS)));
        results.getAll().stream().forEachOrdered(r -> logger.info("DAG Status", Map.of(PIPELINE_ID, pipelineId, "dagName", r.getId(), "status", String.valueOf(r.getStatus()), "result", String.valueOf(r.getResult()), "startTime", String.valueOf(r.getStartTime()), "endTime", String.valueOf(r.getEndTime()))));
        boolean pipelineStatus = results.getAll().stream().allMatch(r -> r.isSuccess() && r.getResult() != null && (r.getResult() == State.DONE || r.getResult() == State.CANCELLED));
        PIPELINE_STATUS.set(pipelineStatus ? "COMPLETED" : "FAILED");
        logger.info("Overall Pipeline status", Map.of(PIPELINE_ID, pipelineId, "status", pipelineStatus));
        String action = env(APP_ON_EXECUTION_ACTION, null);
        if ("exit".equalsIgnoreCase(action) || isBlank(action)) {
            System.exit(pipelineStatus ? 0 : 1);
        } else if ("sleep".equalsIgnoreCase(action)) {
            while (true) {
                Thread.sleep(Long.MAX_VALUE);
            }
        }
    }

    private static void constructFlowGraph(final StargatePipeline stargatePipeline, final Map<String, DirectedAcyclicGraph<BeamVertx, BeamEdge>> dagMap, final DefaultDexecutor<String, State> executor, final Logger logger) {
        DirectedAcyclicGraph<FlowVertex, FlowEdge> graph = new DirectedAcyclicGraph<>(createSupplier(FlowVertex.class), createSupplier(FlowEdge.class), false, true);
        DOTImporter<FlowVertex, FlowEdge> importer = new DOTImporter<>();
        importer.addVertexAttributeConsumer((p, a) -> p.getFirst().setValue(a.getValue()));
        String jobGraph = digraph(stargatePipeline.getJobGraph());
        logger.debug("\n\n----------- JOB GRAPH START -----------\n\n" + jobGraph + "\n\n----------- JOB GRAPH END -----------\n");
        System.out.println("----------- JOB GRAPH START -----------");
        System.out.println(jobGraph);
        System.out.println("----------- JOB GRAPH END -----------");
        importer.importGraph(graph, new StringReader(jobGraph));
        List<FlowVertex> starters = graph.vertexSet().stream().filter(v -> graph.getAncestors(v).isEmpty()).collect(Collectors.toList());
        Set<String> starterFlows = starters.stream().map(v -> v.getValue()).collect(Collectors.toSet());
        for (Map.Entry<String, DirectedAcyclicGraph<BeamVertx, BeamEdge>> entry : dagMap.entrySet()) {
            if (starterFlows.contains(entry.getValue()) || entry.getKey().equalsIgnoreCase(DEFAULT_DAG_NAME)) {
                continue;
            }
            executor.addIndependent(entry.getKey());
        }
        starters.stream().forEach(starter -> linkFlows(executor, graph, starter));
        if (dagMap.containsKey(DEFAULT_DAG_NAME)) {
            executor.addAsDependentOnAllLeafNodes(DEFAULT_DAG_NAME);
        }
    }

    private static void constructFlowGraphForFlink(final StargatePipeline stargatePipeline, final Map<String, DirectedAcyclicGraph<FlinkVertx, FlinkEdge>> dagMap, final DefaultDexecutor<String, State> executor, final Logger logger) {
        DirectedAcyclicGraph<FlowVertex, FlowEdge> graph = new DirectedAcyclicGraph<>(createSupplier(FlowVertex.class), createSupplier(FlowEdge.class), false, true);
        DOTImporter<FlowVertex, FlowEdge> importer = new DOTImporter<>();
        importer.addVertexAttributeConsumer((p, a) -> p.getFirst().setValue(a.getValue()));
        String jobGraph = digraph(stargatePipeline.getJobGraph());
        logger.debug("\n\n----------- Flink JOB GRAPH START -----------\n\n" + jobGraph + "\n\n----------- JOB GRAPH END -----------\n");
        System.out.println("----------- Flink JOB GRAPH START -----------");
        System.out.println(jobGraph);
        System.out.println("----------- Flink JOB GRAPH END -----------");
        importer.importGraph(graph, new StringReader(jobGraph));
        List<FlowVertex> starters = graph.vertexSet().stream().filter(v -> graph.getAncestors(v).isEmpty()).collect(Collectors.toList());
        Set<String> starterFlows = starters.stream().map(v -> v.getValue()).collect(Collectors.toSet());
        for (Map.Entry<String, DirectedAcyclicGraph<FlinkVertx, FlinkEdge>> entry : dagMap.entrySet()) {
            if (starterFlows.contains(entry.getValue()) || entry.getKey().equalsIgnoreCase(DEFAULT_DAG_NAME)) {
                continue;
            }
            executor.addIndependent(entry.getKey());
        }
        starters.stream().forEach(starter -> linkFlows(executor, graph, starter));
        if (dagMap.containsKey(DEFAULT_DAG_NAME)) {
            executor.addAsDependentOnAllLeafNodes(DEFAULT_DAG_NAME);
        }
    }

    private static void linkFlows(final DefaultDexecutor<String, State> executor, final DirectedAcyclicGraph<FlowVertex, FlowEdge> graph, final FlowVertex vertex) {
        if (vertex.init()) return;
        vertex.init(true);
        for (FlowEdge edge : graph.edgesOf(vertex)) {
            FlowVertex target = edge.target();
            if (target == vertex) {
                continue;
            }
            executor.addDependency(vertex.getValue(), target.getValue());
            linkFlows(executor, graph, target);
        }
    }

    public static void overrideAppIdAndApplyDefaults(final String[] args) throws Exception {
        if (Arrays.stream(args).anyMatch(arg -> arg != null && !arg.isEmpty() && arg.contains(PIPELINE_APP_ID_ARG))) {
            System.setProperty(APP_ID, Arrays.stream(args).filter(arg -> arg.contains(PIPELINE_APP_ID_ARG)).findFirst().get().trim().split("=")[1].trim());
        }
        System.setProperty("IGNITE_NO_ASCII", "true");
        System.setProperty("IGNITE_QUIET", "true");
        System.setProperty("IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED", "true");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");
        if (Arrays.stream(args).anyMatch(arg -> arg != null && !arg.isEmpty() && arg.contains(PIPELINE_SYSTEM_PROPERTIES_FILE))) {
            File propsFile = new File(Arrays.stream(args).filter(arg -> arg.contains(PIPELINE_SYSTEM_PROPERTIES_FILE)).findFirst().get().trim().split("=")[1].trim());
            if (propsFile.exists() && !propsFile.isDirectory()) {
                try (InputStream input = new FileInputStream(propsFile.getAbsolutePath())) {
                    Properties properties = new Properties();
                    properties.load(input);
                    properties.forEach((k, v) -> System.setProperty(String.valueOf(k).trim(), String.valueOf(v)));
                }
            }
        }
    }

    @Override
    public void close() {

    }
}
