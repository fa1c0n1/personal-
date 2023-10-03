package org.apache.beam.runners.flink;

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.FileStagingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

public interface FlinkStargateOptions extends FlinkPipelineOptions, PipelineOptions, ApplicationNameOptions, StreamingOptions, FileStagingOptions {

    @Default.Boolean(false)
    Boolean isAsyncEnabled();

    void setAsyncEnabled(Boolean asyncEnabled);

    @Default.String(AUTO)
    String getFlinkHost();

    void setFlinkHost(String value);

    @Default.String("")
    String getFlinkExecutionConfig();

    void setFlinkExecutionConfig(String flinkExecutionConfig);
}