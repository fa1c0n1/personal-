package com.apple.aml.stargate.common.configs;

import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class RunnerResourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, Object> ports;
    private int livenessPort;
    private Map<String, Object> labels;
    @Optional
    private String command;
    @Optional
    private List<String> arguments;

    @SuppressWarnings("unchecked")
    public void init() throws Exception {
        Map<String, Integer> cports = new HashMap<>();
        ports.forEach((k, v) -> cports.put(k, Integer.parseInt(v.toString())));
        ports = (Map) cports;
        Map<String, String> clabels = new HashMap<>();
        labels.forEach((k, v) -> clabels.put(k, String.valueOf(v)));
        labels = (Map) clabels;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Integer> ports() {
        return (Map<String, Integer>) ((Map) ports);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> labels() {
        return (Map<String, String>) ((Map) labels);
    }
}
