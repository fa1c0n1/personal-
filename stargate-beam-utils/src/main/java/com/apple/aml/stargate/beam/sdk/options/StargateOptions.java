package com.apple.aml.stargate.beam.sdk.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface StargateOptions extends PipelineOptions {
    @Description("Pipeline ID")
    @Required
    String getPipelineId();

    void setPipelineId(final String pipelineId);

    @Description("Pipeline Token")
    String getPipelineToken(); // TODO : Legacy pipelineToken; we need to replace these occurrences with A3 and get rid of this completely

    void setPipelineToken(final String pipelineToken);

    @Description("Config Path")
    String getConfigPath();

    void setConfigPath(final String configPath);

    @Description("Shared Directory Path")
    String getSharedDirectoryPath();

    void setSharedDirectoryPath(final String sharedDirectoryPath);

    @Description("Enable Beam Metrics Sink")
    @Default.Boolean(false)
    Boolean isMetricSinkEnabled();

    void setMetricSinkEnabled(Boolean metricSinkEnabled);

    @Description("Dry Run")
    @Default.Boolean(false)
    Boolean isDryRun();

    void setDryRun(Boolean dryRun);

    @Description("Pipeline Runner")
    String getPipelineRunner();

    void setPipelineRunner(final String pipelineRunner);

    @Description("PipelineProfile")
    @Default.String("")
    String getPipelineProfile();

    void setPipelineProfile(final String pipelineProfile);

    @Description("Pipeline Invoke Time")
    String getPipelineInvokeTime();

    void setPipelineInvokeTime(final String pipelineInvokeTime);

    @Description("Pipeline Run No")
    @Default.Long(0L)
    Long getPipelineRunNo();

    void setPipelineRunNo(Long pipelineRunNo);

    @Description("Maximum no of attempts")
    @Default.Integer(1)
    Integer getMaxAttempts();

    void setMaxAttempts(Integer maxAttempts);

    @Description("Custom Serializer")
    @Default.Enum("none")
    CustomSerializer getCustomSerializer();

    void setCustomSerializer(final CustomSerializer customSerializer);

    enum CustomSerializer {
        none, fst, kryo
    }
}
