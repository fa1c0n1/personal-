package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.util.Date;

@Data
public class PipelineInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @BsonProperty(value = "_pipelineId")
    private String pipelineId;

    @BsonProperty(value = "appId")
    private Integer appId;

    @BsonProperty(value = "_updatedOn")
    private Date updatedOn;

    @BsonProperty(value = "currentVersion")
    private Integer currentVersion;

    @BsonProperty(value = "runNo")
    private Long runNo;

    @BsonProperty(value = "spec")
    private String spec;

    @BsonProperty(value = "grafanaLink")
    private String grafanaLink;

    @BsonProperty(value = "pipelineMetadata")
    private PipelineMetadata pipelineMetadata;

    @BsonProperty(value = "domain")
    private String domain;

    public void updatePipelineInfo(PipelineInfo pipelineInfo) {
        if (pipelineMetadata == null) {
            pipelineMetadata = pipelineInfo.pipelineMetadata;
        } else if (pipelineInfo.getPipelineMetadata() != null) {
            pipelineMetadata.updatePipelineMetadata(pipelineInfo.getPipelineMetadata());
        }

        if (grafanaLink == null || (null != pipelineInfo.grafanaLink)) {
            grafanaLink = pipelineInfo.grafanaLink;
        }
    }
}
