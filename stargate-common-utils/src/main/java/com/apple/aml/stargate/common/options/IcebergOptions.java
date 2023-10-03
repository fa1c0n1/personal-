package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants.NODE_TYPE;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class IcebergOptions extends S3Options implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private Map<String, Object> tableProperties;
    @Optional
    private boolean commitInPlace;
    @Optional
    private String implementation; // ADT/OSS
    @Optional
    private int commitRetries = 15;
    @Optional
    private long commitRetryWait = 2000;
    @Optional
    private boolean commitToDisk;
    @Optional
    private boolean deleteCommitPathOnSuccess = true;
    @Optional
    private String storageType;
    @Optional
    private String catalogType;
    @Optional
    private Map<String, Object> catalogProperties;
    @Optional
    private boolean enableObjectStorage = true;
    @Optional
    private boolean useDataFileWriter;
    @Optional
    private int commitPartitions;
    @Optional
    private int commitBatchSize;
    @Optional
    private Duration commitBatchDuration = Duration.ofSeconds(30);
    @Optional
    private boolean useDirectBufferForCommitBatch = true;
    @Optional
    private boolean isDynamic;
    @Optional
    private int flinkParallelism;
    @Optional
    private String splitKey;
    @Optional
    private String splitKeyValues;
    @Optional
    private String distributionMode = "NONE";


    @Override
    public String tmpPath(final String pipelineId) {
        if (storageType() == NODE_TYPE.Hdfs) {
            return hdfsTmpPath(this.getTmpPath(), this.getBasePath(), this.getDefaultFsName(), this.getPrincipal(), pipelineId);
        }
        return super.tmpPath(pipelineId);
    }

    public PipelineConstants.NODE_TYPE storageType() {
        PipelineConstants.NODE_TYPE type = PipelineConstants.NODE_TYPE.nodeType(this.getStorageType());
        if (type == null) {
            return PipelineConstants.NODE_TYPE.Hdfs;
        }
        return type;
    }

    @Override
    public String basePath() {
        if (storageType() == NODE_TYPE.Hdfs) {
            return hdfsBasePath(this.getBasePath(), this.getDefaultFsName(), this.getPrincipal());
        }
        return super.basePath();
    }

    public PipelineConstants.CATALOG_TYPE catalogType() {
        if (isBlank(this.getCatalogType())) return PipelineConstants.CATALOG_TYPE.hive;
        PipelineConstants.CATALOG_TYPE type = PipelineConstants.CATALOG_TYPE.valueOf(this.getCatalogType().toLowerCase());
        if (type == null) {
            return PipelineConstants.CATALOG_TYPE.hive;
        }
        return type;
    }

    public boolean enableLocation() {
        return storageType() != PipelineConstants.NODE_TYPE.S3 || !isEnableObjectStorage();
    }

}
