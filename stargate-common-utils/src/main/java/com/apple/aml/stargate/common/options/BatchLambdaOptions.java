package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class BatchLambdaOptions extends WindowOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private boolean async;
    @Optional
    private Map<String, Object> context;
    @Optional
    private Map<String, Object> contextSecrets; // this is meant for adding via secrets store; all keys are merged into `context` object at runtime
    @Optional
    private String logContext;
    @Optional
    private DhariOptions dhariOptions;
    @Optional
    private AttributesOptions attributesOptions;
    @Optional
    private SolrOptions solrOptions;
    @Optional
    private SnowflakeOptions snowflakeOptions;
    @Optional
    private JdbiOptions jdbcOptions;
    @Optional
    private RetryOptions retryOptions;
    @Optional
    private HttpInvokerOptions restOptions;
    @Optional
    private Map<String, Object> restServiceOptions; // key as serviceId (any string ) vs value as HttpInvokerOptions ( since hocon supports only Map<String, Object> wrapper is used )
    // following options are associated with derive schema Options; should be in sync with DerivedSchemaOptions
    @Optional
    private String schemaType;
    @Optional
    private Object schema;
    @Optional
    private Object appendSchema;
    @Optional
    private Object replaceSchema;
    @Optional
    private Object schemaIncludes;
    @Optional
    private Object schemaExcludes;
    @Optional
    private String schemaRegex;
    @Optional
    private boolean enableSimpleSchema;
    @Optional
    private boolean enableHierarchicalSchemaFilters;

    public Map<String, Object> context() {
        Map<String, Object> map = contextSecrets == null ? new HashMap<>() : new HashMap<>(contextSecrets);
        if (context != null) map.putAll(context);
        return map;
    }
}
