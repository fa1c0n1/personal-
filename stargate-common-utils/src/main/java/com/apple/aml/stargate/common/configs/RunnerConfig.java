package com.apple.aml.stargate.common.configs;

import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class RunnerConfig implements Serializable {
    public static final String CONFIG_NAME = "stargate.runner.config";
    private static final long serialVersionUID = 1L;
    private String className;
    @Optional
    private String version;
    private RunnerResourceConfig coordinator;
    private RunnerResourceConfig worker;
    @Optional
    private Map<String, Object> configData;
    @Optional
    private Map<String, Object> env;

    @SuppressWarnings("unchecked")
    public void init() throws Exception {
        coordinator.init();
        worker.init();
        if (env == null) env = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<String, String>> configData() {
        return (Map<String, Map<String, String>>) ((Map) configData);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> env() {
        return (Map<String, String>) ((Map) env);
    }

}
