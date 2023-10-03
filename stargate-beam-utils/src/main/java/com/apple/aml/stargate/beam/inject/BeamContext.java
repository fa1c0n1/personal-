package com.apple.aml.stargate.beam.inject;

import com.apple.aml.stargate.common.utils.ContextHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.beam.sdk.transforms.DoFn;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class BeamContext extends ContextHandler.Context {
    private DoFn.WindowedContext windowedContext;
}
