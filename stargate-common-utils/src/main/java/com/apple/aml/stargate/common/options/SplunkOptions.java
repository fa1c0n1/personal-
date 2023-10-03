package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class SplunkOptions extends WindowOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String commType = "HEC";
    @Optional
    private String url;
    @Optional
    private String token;
    @Optional
    private int batchCount;
    @Optional
    private int parallelism;
    @Optional
    private String timeAttribute;
    @Optional
    private String hostAttribute;
    @Optional
    private String host;
    @Optional
    private String sourceAttribute;
    @Optional
    private String source;
    @Optional
    private String sourceTypeAttribute;
    @Optional
    private String sourceType;
    @Optional
    private String indexAttribute;
    @Optional
    private String index;
    @Optional
    private String expression;
    @Optional
    private boolean kvPairs;
    @Optional
    private boolean parse;
    @Optional
    private boolean emitSuccessRecords;
    @Optional
    private String fileName;
    @Optional
    private String fileNamePattern;
    @Optional
    private String maxFileSize;
    @Optional
    private String maxHistory;
    @Optional
    private String totalSizeCap;
    @Optional
    private String newLineReplacement;
    @Optional
    private boolean emitHECEvent;
    @Optional
    private Map<String, Object> headers = new HashMap<>();
    @Optional
    private HttpConnectionOptions connectionOptions;
    @Optional
    private boolean skipNullValues;

    public int parallelism() {
        return parallelism > 0 ? parallelism : getPartitions();
    }

    public int batchCount() {
        return batchCount > 0 ? batchCount : getBatchSize();
    }
}
