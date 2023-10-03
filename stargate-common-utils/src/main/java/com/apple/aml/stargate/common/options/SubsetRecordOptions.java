package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class SubsetRecordOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private Map<String, Object> mappings;
    @Optional
    private boolean passThrough;
    @Optional
    private Object includes;
    @Optional
    private Object excludes;
    @Optional
    private String regex;
    @Optional
    private boolean enableSimpleSchema;
    @Optional
    private boolean enableHierarchicalSchemaFilters;
}
