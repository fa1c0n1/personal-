package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CUSTOM_FIELD_DELIMITER;

@Data
@EqualsAndHashCode(callSuper = true)
public class EnhanceRecordOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private List<Object> mappings;
    @Optional
    private Map<String, Object> schemaMappings;
    @Optional
    private boolean passThrough;
    @Optional
    private String delimiter = CUSTOM_FIELD_DELIMITER;
}
