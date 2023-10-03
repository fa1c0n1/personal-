package com.apple.aml.stargate.common.options;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;


@Data
@EqualsAndHashCode(callSuper = true)
public class DeploymentOptions extends DeploymentOperatorOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    @JsonAlias({"jobManager", "driver"})
    private DeploymentResourceOptions coordinator;
    @Optional
    @JsonAlias({"taskManager", "executor"})
    private DeploymentResourceOptions worker;
    @Optional
    @JsonAlias({"logOverrides"})
    private Map<String, Object> logLevels;
    @Optional
    private Map<String, Object> properties; // Can be set only during create/update data pipeline definition; always persisted; will be ignored elsewhere
    @Optional
    @JsonAlias({"props"})
    private Map<String, Object> overrides; // Can be set only during fetch manifest; never persisted; will be ignored elsewhere
    @Optional
    private Map<String, Object> arguments;
    @Optional
    @JsonAlias({"flinkConfigs", "sparkConfigs"})
    private Map<String, Object> runnerConfigs;
    @Optional
    private Map<String, Object> definition;
    @Optional
    private Map<String, Object> nodeSelector;
    @Optional
    private Map<String, Object> whisperOptions;
    @Optional
    @JsonAlias({"envMap"})
    private Map<String, Object> globals;
    @Optional
    private Map<String, Object> labels;
    @Optional
    private Map<String, Object> annotations;

}
