package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.avro.reflect.Nullable;

import java.io.Serializable;
import java.time.Duration;

@Data
@EqualsAndHashCode(callSuper = true)
public class WindowOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private Duration windowDuration = Duration.ofHours(1);
    @Optional
    private boolean triggering = true;
    @Optional
    private String windowAccumulationMode;
    @Optional
    private String triggerMode;
    @Optional
    private String windowClosingBehavior;
    @Optional
    private int batchSize;
    @Optional
    private String batchBytes;
    @Optional
    @Nullable
    private String partitionStrategy;
    @Optional
    private boolean appendPartitionNoToKey;
    @Optional
    private int partitions;
    @Optional
    private Duration batchDuration;
    @Optional
    private Duration windowLateness = Duration.ofSeconds(10);
    @Optional
    private boolean statefulBatch = true;

    public Duration batchDuration() {
        if (batchDuration != null && batchDuration.getSeconds() > 0) {
            return batchDuration;
        }
        if (windowDuration != null && windowDuration.getSeconds() > 0) {
            return windowDuration;
        }
        return Duration.ofSeconds(3 * 60);
    }
}
