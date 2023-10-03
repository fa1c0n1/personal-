package com.apple.aml.stargate.common.pojo;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.options.DeploymentOperatorOptions;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

import java.io.Serializable;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.merge;
import static com.apple.aml.stargate.common.utils.JsonUtils.nonNullValueMap;
import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class CoreOptions extends DeploymentOperatorOptions implements Serializable { // Typesafe Hocon doesn't support Map with Value!=Object; hence the need for this class
    private static final long serialVersionUID = 1L;
    @Optional
    @JsonAlias({"jobManager", "driver"})
    private CoreResourceOptions coordinator;
    @Optional
    @JsonAlias({"taskManager", "executor"})
    private CoreResourceOptions worker;
    @Optional
    @JsonAlias({"logOverrides"})
    private Map<String, String> logLevels;
    @Optional
    @JsonAlias({"props"})
    private Map<String, String> overrides; // Can be set only during fetch manifest; never persisted; will be ignored elsewhere
    @Optional
    private Map<String, String> arguments;
    @Optional
    @JsonAlias({"flinkConfigs", "sparkConfigs"})
    private Map<String, String> runnerConfigs;
    @Optional
    private JsonNode definition;
    @Optional
    private Map<String, String> nodeSelector;
    @Optional
    private Map<String, String> whisperOptions;
    @Optional
    @JsonAlias({"envMap"})
    private Map<String, String> globals;
    @Optional
    private Map<String, String> labels;
    @Optional
    private Map<String, String> annotations;

    public CoreResourceOptions coordinator() throws Exception {
        CoreResourceOptions resource = merge(new CoreResourceOptions(), jsonString(nonNullValueMap(this)));
        resource.setEnv(null);
        resource.setLabels(null);
        resource.setAnnotations(null);
        if (this.coordinator != null) {
            resource = merge(resource, jsonString(nonNullValueMap(this.coordinator)));
            resource.setEnv(this.coordinator.getEnv());
            resource.setLabels(this.coordinator.getLabels());
            resource.setAnnotations(this.coordinator.getAnnotations());
        }
        if (isBlank(resource.getMemory())) resource.setMemory(this.getCoordinatorMemory());
        if (isBlank(resource.getCpu())) resource.setCpu(this.getCoordinatorCPU());
        if (isBlank(resource.getMemoryLimit())) resource.setMemoryLimit(this.getCoordinatorMemoryLimit());
        if (isBlank(resource.getCpuLimit())) resource.setCpuLimit(this.getCoordinatorCPULimit());
        if (isBlank(resource.getMemoryLimit())) resource.setMemoryLimit(resource.getMemory());
        if (isBlank(resource.getCpuLimit())) resource.setCpuLimit(resource.getCpu());
        resource.enableDefaults();
        return resource;
    }

    public CoreResourceOptions worker() throws Exception {
        CoreResourceOptions resource = merge(new CoreResourceOptions(), jsonString(nonNullValueMap(this)));
        resource.setEnv(null);
        resource.setLabels(null);
        resource.setAnnotations(null);
        if (this.worker != null) {
            resource = merge(resource, jsonString(nonNullValueMap(this.worker)));
            resource.setEnv(this.worker.getEnv());
            resource.setLabels(this.worker.getLabels());
            resource.setAnnotations(this.worker.getAnnotations());
        }
        if (isBlank(resource.getMemory())) resource.setMemory(this.getWorkerMemory());
        if (isBlank(resource.getCpu())) resource.setCpu(this.getWorkerCPU());
        if (isBlank(resource.getMemoryLimit())) resource.setMemoryLimit(this.getWorkerMemoryLimit());
        if (isBlank(resource.getCpuLimit())) resource.setCpuLimit(this.getWorkerCPULimit());
        if (isBlank(resource.getMemoryLimit())) resource.setMemoryLimit(resource.getMemory());
        if (isBlank(resource.getCpuLimit())) resource.setCpuLimit(resource.getCpu());
        if (resource.getReplicas() == null || resource.getReplicas() <= 0) resource.setReplicas(this.getNumberOfWorkers());
        resource.enableDefaults();
        return resource;
    }

    @SneakyThrows
    public void calculateParallelism() {
        int parallelism = this.getParallelism();
        int numberOfSlots = this.getNumberOfSlots();
        if (parallelism <= 0 && numberOfSlots <= 0) throw new InvalidInputException("missing/invalid value for - parallelism/numberOfSlots");
        if (parallelism <= 0) {
            parallelism = this.getNumberOfWorkers() * numberOfSlots;
            this.setParallelism(parallelism);
        } else if (numberOfSlots <= 0) {
            numberOfSlots = (int) Math.ceil((this.getParallelism() * 1.0) / this.getNumberOfWorkers());
            this.setNumberOfSlots(numberOfSlots);
        }
    }

    public String functionalDAG() {
        return isBlank(this.getSpec()) ? this.getDefinition() == null ? null : this.getDefinition().toPrettyString() : this.getSpec();
    }
}
