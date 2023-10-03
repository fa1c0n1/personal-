package com.apple.aml.stargate.common.options;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class ResourceOptions extends CPUMemOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    @JsonAlias({"numberOfTaskManagers", "numberOfExecutors", "numberOfPods", "noOfTaskManagers", "noOfExecutors", "noOfPods"})
    protected Integer replicas;
    @Optional
    private String memory;
    @Optional
    private String cpu;
    @Optional
    private String memoryLimit;
    @Optional
    private String cpuLimit;
    @Optional
    @JsonAlias({"splunkForwarder"})
    private String logForwarder;
    @Optional
    private String apmMode;
    @Optional
    private String debugMode;
    @Optional
    private List<String> jvmOptions;
    @Optional
    private List<Map<String, Object>> env;

    public void enableDefaults() {
        if (isBlank(cpuLimit)) cpuLimit = cpu;
        if (isBlank(memoryLimit)) memoryLimit = memory;
        if (isBlank(splunkCPULimit)) splunkCPULimit = splunkCPU;
        if (isBlank(splunkMemoryLimit)) splunkMemoryLimit = splunkMemory;
        if (isBlank(initCPULimit)) initCPULimit = initCPU;
        if (isBlank(initMemoryLimit)) initMemoryLimit = initMemory;
        if (isBlank(pythonCPULimit)) pythonCPULimit = pythonCPU;
        if (isBlank(pythonMemoryLimit)) pythonMemoryLimit = pythonMemory;
        if (isBlank(externalJvmCPULimit)) externalJvmCPULimit = externalJvmCPU;
        if (isBlank(externalJvmMemoryLimit)) externalJvmMemoryLimit = externalJvmMemory;
        if (isBlank(externalCPULimit)) externalCPULimit = externalCPU;
        if (isBlank(externalMemoryLimit)) externalMemoryLimit = externalMemory;
        if (!guaranteedQos()) return;
        cpu = cpuLimit;
        memory = memoryLimit;
        splunkCPU = splunkCPULimit;
        splunkMemory = splunkMemoryLimit;
        initCPU = initCPULimit;
        initMemory = initMemoryLimit;
        pythonCPU = pythonCPULimit;
        pythonMemory = pythonMemoryLimit;
        externalJvmCPU = externalJvmCPULimit;
        externalJvmMemory = externalJvmMemoryLimit;
        externalCPU = externalCPULimit;
        externalMemory = externalMemoryLimit;
    }
}
