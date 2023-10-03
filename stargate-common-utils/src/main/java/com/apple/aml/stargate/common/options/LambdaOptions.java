package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class LambdaOptions extends DerivedSchemaOptions implements Serializable {
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

    public Map<String, Object> context() {
        Map<String, Object> map = contextSecrets == null ? new HashMap<>() : new HashMap<>(contextSecrets);
        if (context != null) map.putAll(context);
        return map;
    }
}
