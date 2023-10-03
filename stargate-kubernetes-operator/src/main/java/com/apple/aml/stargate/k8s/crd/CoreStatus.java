package com.apple.aml.stargate.k8s.crd;

import io.javaoperatorsdk.operator.api.ObservedGenerationAwareStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = false)
public class CoreStatus extends ObservedGenerationAwareStatus implements Serializable {
    private static final long serialVersionUID = 1L;
    private String requestId;
    private long appId;
    private String pipelineId;
    private long runNo;
    private long versionNo;
    private String mode;
    private String runner;
    private String stargateVersion;
    private String sharedToken;
    private String startedOn;
    private String updatedOn;
    private String status;
    private String encodedDAG;
    private String encodedOptions;

    @Override
    public String toString() {
        return "CoreStatus{" + "requestId='" + requestId + '\'' + ", appId=" + appId + ", pipelineId='" + pipelineId + '\'' + ", runNo=" + runNo + ", versionNo=" + versionNo + ", mode='" + mode + '\'' + ", stargateVersion='" + stargateVersion + '\'' + ", sharedToken='" + sharedToken + '\'' + ", startedOn='" + startedOn + '\'' + ", updatedOn='" + updatedOn + '\'' + ", status='" + status + '\'' + '}';
    }
}
