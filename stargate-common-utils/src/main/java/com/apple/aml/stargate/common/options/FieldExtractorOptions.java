package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@EqualsAndHashCode(callSuper = true)
@Data
public class FieldExtractorOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private Object fields; // can be a CSV ( similar to csv of targetFieldName, sourceFieldMapping, defaultSchema(optional), nullable(optional) ) or Map of targetFieldName->rest of fields
    @Optional
    private String expression;
}
