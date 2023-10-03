package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class FileOptions extends SchemaMappingOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String basePath;
    @Optional
    private String tmpPath;
    @Optional
    private String fileFormat;
    @Optional
    private String fileCompression;
    @Optional
    private KVFilterOptions outputFilter;
    @Optional
    private boolean combine;
    @Optional
    private String combinerSchemaId;
    @Optional
    private boolean shuffle;
    @Optional
    private boolean useBulkWriter;
    @Optional
    private boolean useDirectBuffer;
    @Optional
    private String filePattern;
    @Optional
    private String recordIdentifier;
    @Optional
    private String filePath;

    public String tmpPath(final String pipelineId) {
        if (isBlank(this.getTmpPath())) {
            return basePath();
        }
        return this.getTmpPath();
    }

    public String basePath() {
        return this.getBasePath();
    }

    public DATA_FORMAT fileFormat() {
        if (isBlank(this.fileFormat)) {
            return DATA_FORMAT.parquet;
        }
        try {
            DATA_FORMAT format = DATA_FORMAT.valueOf(this.fileFormat.trim().toLowerCase());
            if (format != null) {
                return format;
            }
            return DATA_FORMAT.parquet;
        } catch (Exception e) {
            return DATA_FORMAT.parquet;
        }
    }
}
