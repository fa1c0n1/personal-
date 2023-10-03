package com.apple.aml.stargate.flink.inject;

import com.apple.aml.stargate.common.utils.ContextHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.beam.sdk.transforms.DoFn;

@Data
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class FlinkContext extends ContextHandler.Context {

}
