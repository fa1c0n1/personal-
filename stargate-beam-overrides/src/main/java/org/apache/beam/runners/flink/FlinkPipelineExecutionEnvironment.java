package org.apache.beam.runners.flink;

import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.apple.jvm.commons.util.Strings.isBlank;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * The class that instantiates and manages the execution of a given job. Depending on if the job is
 * a Streaming or Batch processing one, it creates the adequate execution environment ({@link
 * ExecutionEnvironment} or {@link StreamExecutionEnvironment}), the necessary {@link
 * FlinkPipelineTranslator} ({@link FlinkBatchPipelineTranslator} or {@link
 * FlinkStreamingPipelineTranslator}) to transform the Beam job into a Flink one, and executes the
 * (translated) job.
 */
@SuppressWarnings({"nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class FlinkPipelineExecutionEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPipelineExecutionEnvironment.class);

    private final FlinkPipelineOptions options;

    /**
     * The Flink Batch execution environment. This is instantiated to either a {@link
     * org.apache.flink.api.java.CollectionEnvironment}, a {@link
     * org.apache.flink.api.java.LocalEnvironment} or a {@link
     * org.apache.flink.api.java.RemoteEnvironment}, depending on the configuration options.
     */
    private ExecutionEnvironment flinkBatchEnv;

    /**
     * The Flink Streaming execution environment. This is instantiated to either a {@link
     * org.apache.flink.streaming.api.environment.LocalStreamEnvironment} or a {@link
     * org.apache.flink.streaming.api.environment.RemoteStreamEnvironment}, depending on the
     * configuration options, and more specifically, the url of the leader.
     */
    private StreamExecutionEnvironment flinkStreamEnv;

    /**
     * Creates a {@link FlinkPipelineExecutionEnvironment} with the user-specified parameters in the
     * provided {@link FlinkPipelineOptions}.
     *
     * @param options the user-defined pipeline options.
     */
    FlinkPipelineExecutionEnvironment(FlinkPipelineOptions options) {
        this.options = checkNotNull(options);
    }

    /**
     * Launches the program execution.
     */
    @SuppressWarnings("unchecked")
    public JobExecutionResult executePipeline() throws Exception {
        final String jobName = options.getJobName();

        if (flinkBatchEnv == null && flinkStreamEnv == null) {
            throw new IllegalStateException("The Pipeline has not yet been translated.");
        }
        ExecutionConfig config = flinkBatchEnv == null ? flinkStreamEnv.getConfig() : flinkBatchEnv.getConfig();
        FlinkStargateOptions stargateOptions = options.as(FlinkStargateOptions.class);
        if (!isBlank(stargateOptions.getFlinkExecutionConfig())) {
            Class configClass = config.getClass();
            for (String token : stargateOptions.getFlinkExecutionConfig().trim().split(";")) {
                String[] split = token.trim().split("=");
                String name = split[0].trim();
                if (isBlank(name)) continue;
                try {
                    if (split.length == 1 || isBlank(split[1])) {
                        configClass.getMethod(name).invoke(config);
                    } else if (!isBlank(split[1])) {
                        BeanUtils.setProperty(config, name, split[1].trim());
                    }
                } catch (Exception e) {
                    LOG.warn("Could not set flink execution config {}. Reason - {}", name, e.getMessage());
                }
            }
        }

        if (flinkBatchEnv != null) {
            if (stargateOptions.isAsyncEnabled()) {
                JobClient jobClient = flinkBatchEnv.executeAsync(jobName);
                return jobClient.getJobExecutionResult().get();
            }
            return flinkBatchEnv.execute(jobName);
        } else if (flinkStreamEnv != null) {
            if (stargateOptions.isAsyncEnabled()) {
                JobClient jobClient = flinkStreamEnv.executeAsync(jobName);
                return jobClient.getJobExecutionResult().get();
            }
            return flinkStreamEnv.execute(jobName);
        } else {
            throw new IllegalStateException("The Pipeline has not yet been translated.");
        }
    }

    /**
     * Retrieves the generated JobGraph which can be submitted against the cluster. For testing
     * purposes.
     */
    @VisibleForTesting
    JobGraph getJobGraph(Pipeline p) {
        translate(p);
        StreamGraph streamGraph = flinkStreamEnv.getStreamGraph();
        // Normally the job name is set when we execute the job, and JobGraph is immutable, so we need
        // to set the job name here.
        streamGraph.setJobName(p.getOptions().getJobName());
        return streamGraph.getJobGraph();
    }

    /**
     * Depending on if the job is a Streaming or a Batch one, this method creates the necessary
     * execution environment and pipeline translator, and translates the {@link
     * org.apache.beam.sdk.values.PCollection} program into a {@link
     * org.apache.flink.api.java.DataSet} or {@link
     * org.apache.flink.streaming.api.datastream.DataStream} one.
     */
    public void translate(Pipeline pipeline) {
        this.flinkBatchEnv = null;
        this.flinkStreamEnv = null;

        final boolean hasUnboundedOutput = PipelineTranslationModeOptimizer.hasUnboundedOutput(pipeline);
        if (hasUnboundedOutput) {
            LOG.info("Found unbounded PCollection. Switching to streaming execution.");
            options.setStreaming(true);
        }

        // Staged files need to be set before initializing the execution environments
        prepareFilesToStageForRemoteClusterExecution(options);

        FlinkPipelineTranslator translator;
        if (options.isStreaming()) {
            this.flinkStreamEnv = FlinkExecutionEnvironments.createStreamExecutionEnvironment(options);
            if (hasUnboundedOutput && !flinkStreamEnv.getCheckpointConfig().isCheckpointingEnabled()) {
                LOG.warn("UnboundedSources present which rely on checkpointing, but checkpointing is disabled.");
            }
            translator = new FlinkStreamingPipelineTranslator(flinkStreamEnv, options);
        } else {
            this.flinkBatchEnv = FlinkExecutionEnvironments.createBatchExecutionEnvironment(options);
            translator = new FlinkBatchPipelineTranslator(flinkBatchEnv, options);
        }

        // Transform replacements need to receive the finalized PipelineOptions
        // including execution mode (batch/streaming) and parallelism.
        pipeline.replaceAll(FlinkTransformOverrides.getDefaultOverrides(options));

        translator.translate(pipeline);
    }

    /**
     * Local configurations work in the same JVM and have no problems with improperly formatted files
     * on classpath (eg. directories with .class files or empty directories). Prepare files for
     * staging only when using remote cluster (passing the leader address explicitly).
     */
    private static void prepareFilesToStageForRemoteClusterExecution(FlinkPipelineOptions options) {
        if (!options.getFlinkMaster().matches("\\[auto\\]|\\[collection\\]|\\[local\\]")) {
            PipelineResources.prepareFilesForStaging(options);
        }
    }

    @VisibleForTesting
    ExecutionEnvironment getBatchExecutionEnvironment() {
        return flinkBatchEnv;
    }

    @VisibleForTesting
    StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return flinkStreamEnv;
    }
}
