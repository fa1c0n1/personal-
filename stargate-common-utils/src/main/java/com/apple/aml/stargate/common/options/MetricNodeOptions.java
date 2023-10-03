package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EqualsAndHashCode(callSuper = true)
@Data
public class MetricNodeOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private String expression;
    @Optional
    private Object counters;
    @Optional
    private Object histograms;
    @Optional
    private Object gauges;

    @SuppressWarnings("unchecked")
    public Set<String> counters() {
        return this.counters == null ? new HashSet<>() : new HashSet<>(counters instanceof List ? (List<String>) counters : List.of(((String) counters).split(",")));
    }

    @SuppressWarnings("unchecked")
    public Set<String> histograms() {
        return this.histograms == null ? new HashSet<>() : new HashSet<>(histograms instanceof List ? (List<String>) histograms : List.of(((String) histograms).split(",")));
    }

    @SuppressWarnings("unchecked")
    public Set<String> gauges() {
        return this.gauges == null ? new HashSet<>() : new HashSet<>(gauges instanceof List ? (List<String>) gauges : List.of(((String) gauges).split(",")));
    }
}
