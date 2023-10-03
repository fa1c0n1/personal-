package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;

import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;

@Data
public class CPUMemOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    protected String splunkMemory;
    @Optional
    protected String splunkCPU;
    @Optional
    protected String initMemory;
    @Optional
    protected String initCPU;
    @Optional
    protected String pythonMemory;
    @Optional
    protected String pythonCPU;
    @Optional
    protected String externalJvmMemory;
    @Optional
    protected String externalJvmCPU;
    @Optional
    protected String externalMemory;
    @Optional
    protected String externalCPU;
    @Optional
    protected String splunkMemoryLimit;
    @Optional
    protected String splunkCPULimit;
    @Optional
    protected String initMemoryLimit;
    @Optional
    protected String initCPULimit;
    @Optional
    protected String pythonMemoryLimit;
    @Optional
    protected String pythonCPULimit;
    @Optional
    protected String externalJvmMemoryLimit;
    @Optional
    protected String externalJvmCPULimit;
    @Optional
    protected String externalMemoryLimit;
    @Optional
    protected String externalCPULimit;
    @Optional
    private String qos;

    public boolean guaranteedQos() {
        return "guaranteed".equalsIgnoreCase(qos) || parseBoolean(qos);
    }
}
