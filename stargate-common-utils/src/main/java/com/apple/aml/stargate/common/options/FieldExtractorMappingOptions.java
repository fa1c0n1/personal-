package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.pojo.SimpleSchema;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonPropertyOrder({"fieldName", "mapping", "fieldType", "mandatory", "documentation", "defaultValue"})
public class FieldExtractorMappingOptions extends SimpleSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String mapping;
}
