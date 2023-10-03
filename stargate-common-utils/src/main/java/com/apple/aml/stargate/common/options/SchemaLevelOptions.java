package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class SchemaLevelOptions extends WindowOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String filePath;
    @Optional
    private String dbName;
    @Optional
    private String tableName;
    @Optional
    private String partitionColumn;
    @Optional
    private List<String> partitionTransforms;
    @Optional
    private String partitionFormat;
    @Optional
    private String sortColumn;
    @Optional
    private List<String> sortColumns;
    @Optional
    private String outputKey;
    @Optional
    private String commitPath;
    @Optional
    private boolean passThrough;
    @Optional
    private List<String> buckets;
    @Optional
    private String transform;
    @Optional
    private String prefix;
    @Optional
    private int prefixCount;
    @Optional
    private String aliasName;
    @Optional
    private String collectionName;
    @Optional
    private String xsvDelimiter;
    @Optional
    private boolean includeXSVHeader;

    public String prefix() {
        if (isBlank(prefix)) {
            if (prefixCount >= 1) {
                return "${util.randomPrefix(schema.fullName, " + prefixCount + ")}";
            }
            return null;
        }
        return prefix.trim();
    }

    public Map<String, Object> logMap() {
        return Map.of("filePath", String.valueOf(filePath), "dbName", String.valueOf(dbName), "tableName", String.valueOf(tableName), "partitionColumn", String.valueOf(partitionColumn), "partitionFormat", String.valueOf(partitionFormat), "sortColumn", String.valueOf(sortColumn), "outputKey", String.valueOf(outputKey), "prefix", String.valueOf(prefix), "collectionName", String.valueOf(collectionName), "aliasName", String.valueOf(aliasName));
    }
}
