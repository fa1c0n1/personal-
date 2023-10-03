package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class HdfsOpsOptions extends HdfsOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String operation;
    @Optional
    private String source;
    @Optional
    private String destination;
    @Optional
    private List<String> paths;
    @Optional
    private boolean sequence = true;
}
