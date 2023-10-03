package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class DataGeneratorOptions extends SequencerOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String keyTemplate;
    @Optional
    private String payloadTemplate;
    @Optional
    private String markerSchemaId;
    @Optional
    private String markerTemplate;
    @Optional
    private String generatorType;
    @Optional
    private int partitions = 1;
    @Optional
    private String samplerType;
    @Optional
    private List<String> filePaths;
    @Optional
    private String compression;
}
