package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.constants.PipelineConstants.DEPLOYMENT_SIZE;
import com.apple.aml.stargate.common.constants.PipelineConstants.DEPLOYMENT_TARGET;
import com.apple.aml.stargate.common.constants.PipelineConstants.RUNNER;
import com.apple.aml.stargate.common.pojo.NameValuePair;
import com.apple.aml.stargate.common.utils.ClassUtils;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.apple.jvm.commons.util.Strings.isBlank;
import static lombok.AccessLevel.NONE;

@Data
@EqualsAndHashCode(callSuper = true)
public class DeploymentOperatorOptions extends CPUMemOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    @JsonAlias({"jobManagerMemory", "driverMemory"})
    protected String coordinatorMemory;
    @Optional
    @JsonAlias({"jobManagerCPU", "driverCPU"})
    protected String coordinatorCPU;
    @Optional
    @JsonAlias({"taskManagerMemory", "executorMemory"})
    protected String workerMemory;
    @Optional
    @JsonAlias({"taskManagerCPU", "executorCPU"})
    protected String workerCPU;
    @Optional
    @JsonAlias({"jobManagerMemoryLimit", "driverMemoryLimit"})
    protected String coordinatorMemoryLimit;
    @Optional
    @JsonAlias({"jobManagerCPULimit", "driverCPULimit"})
    protected String coordinatorCPULimit;
    @Optional
    @JsonAlias({"taskManagerMemoryLimit", "executorMemoryLimit"})
    protected String workerMemoryLimit;
    @Optional
    @JsonAlias({"taskManagerCPULimit", "executorCPULimit"})
    protected String workerCPULimit;
    @Optional
    @JsonAlias({"numberOfTaskManagers", "numberOfExecutors", "numberOfPods", "noOfTaskManagers", "noOfExecutors", "noOfPods"})
    protected int numberOfWorkers;
    @Optional
    @JsonAlias({"numberOfTaskSlots", "numberOfExecutorSlots", "noOfTaskSlots", "noOfExecutorSlots", "noOfSlots", "slotsPerWorker", "slots"})
    protected int numberOfSlots;
    @Optional
    protected String webProxy;
    @JsonIgnore
    @Getter(NONE)
    @Setter(NONE)
    private DEPLOYMENT_SIZE _deploymentSize;
    @Optional
    private String merge;
    @Optional
    private long appId;
    @Optional
    private String pipelineId;
    @Optional
    @JsonAlias({"appMode"})
    private String mode;
    @Optional
    private String runner;
    @Optional
    private String deploymentTarget;
    @Optional
    private String surfaceQueue;
    @Optional
    private String surfaceBasePath;
    @Optional
    @JsonAlias({"clusterSize"})
    private String deploymentSize;
    @Optional
    @JsonAlias({"jobParallelism", "slotsPerJob"})
    private int parallelism;
    @Optional
    @JsonAlias({"splunkForwarder"})
    private String logForwarder;
    @Optional
    private String splunkCluster;
    @Optional
    private String splunkIndex;
    @Optional
    private String apmMode;
    @Optional
    private String debugMode;
    @Optional
    private String serviceAccountName;
    @Optional
    private String pvcClaim;
    @Optional
    private List<Map<String, Object>> env;
    @Optional
    private String secretName;
    @Optional
    private List<String> secretKeys;
    @Optional
    private String awsRegion;
    @Optional
    private String awsDefaultRegion;
    @Optional
    private List<String> jvmOptions;
    @Optional
    private List<NameValuePair> args;
    @Optional
    private String stargateImage;
    @Optional
    @JsonAlias({"customImage", "javaImage"})
    private String image;
    @Optional
    private String jvmImage;
    @Optional
    private String pythonImage;
    @Optional
    private String externalJvmImage;
    @Optional
    private String externalImage;
    @Optional
    @JsonAlias({"customImagePullPolicy"})
    private String imagePullPolicy;
    @Optional
    private String sharedRuntime;
    @Optional
    @JsonAlias({"customVersion"})
    private String version;
    @Optional
    private String splunkVersion;
    @Optional
    private String stargateVersion;
    @Optional
    private String spec;
    @Optional
    private int ndots;
    @Optional
    private String logManifest;
    @Optional
    private String requestId;
    @Optional
    private int backoffLimit;

    public void setDeploymentSize(final String deploymentSize) {
        this.deploymentSize = deploymentSize;
        this._deploymentSize = DEPLOYMENT_SIZE.deploymentSize(deploymentSize);
    }

    public RUNNER runner() {
        if (isBlank(getRunner())) {
            return RUNNER.flink;
        }
        return RUNNER.valueOf(getRunner().trim().toLowerCase());
    }

    public DEPLOYMENT_TARGET deploymentTarget() {
        if (isBlank(getDeploymentTarget())) return DEPLOYMENT_TARGET.stargate;
        try {
            DEPLOYMENT_TARGET target = DEPLOYMENT_TARGET.valueOf(getDeploymentTarget().trim().toLowerCase());
            if (target != null) return target;
        } catch (Exception e) {

        }
        return DEPLOYMENT_TARGET.stargate;
    }

    public DEPLOYMENT_SIZE deploymentSize() {
        return this._deploymentSize;
    }

    public String imagePullPolicy() {
        return isBlank(this.getImagePullPolicy()) ? "IfNotPresent" : this.getImagePullPolicy().trim();
    }

    public boolean logManifest() {
        return ClassUtils.parseBoolean(this.logManifest);
    }

    public int backoffLimit() {
        if (backoffLimit <= 0) return Integer.MAX_VALUE;
        return backoffLimit;
    }

}
