package com.apple.aml.stargate.common.nodes;

import com.apple.aml.stargate.common.annotation.RootLevelOption;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.ErrorOptions;
import com.apple.aml.stargate.common.options.KVFilterOptions;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.typesafe.config.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.util.Arrays.asList;
import static lombok.AccessLevel.NONE;

@Getter
@Setter
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
public class StargateNode<C> extends ErrorOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @EqualsAndHashCode.Include
    @Optional
    private String name;
    @Optional
    private String type;
    @Optional
    private String alias;
    @Optional
    private String environment;
    @Optional
    private C config;
    @Optional
    private boolean active = true;
    @Optional
    private boolean primary;
    @Optional
    private String connectId;
    @Optional
    private Object connectIds;
    @Optional
    private boolean log;
    @Optional
    private String logLevelIn;
    @Optional
    private String logLevelOut;
    @Optional
    private KVFilterOptions filter;
    @Optional
    private boolean distinct;
    @Optional
    private boolean saveStateIn;
    @Optional
    private boolean saveStateOut;
    @Optional
    @RootLevelOption
    private String schemaId;
    @Optional
    @RootLevelOption
    private String schemaReference;
    @Optional
    @RootLevelOption
    private String transform;
    @Optional
    private String metricsExpression;
    @Optional
    private boolean shuffle;
    @Optional
    private int maxTps = 0;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Map<String, byte[]> configFiles;

    public Map<String, byte[]> configFiles() {
        return configFiles;
    }

    public Map<String, byte[]> configFiles(Map<String, byte[]> configFiles) {
        this.configFiles = configFiles;
        return this.configFiles;
    }

    @SuppressWarnings("unchecked")
    public List<String> connectIds() {
        if (connectIds == null || isBlank(connectIds.toString())) {
            return (isBlank(connectId)) ? null : asList(connectId.trim());
        }
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        if (!isBlank(connectId)) {
            ids.add(connectId.trim());
        }
        ids.addAll((connectIds instanceof List) ? (List) connectIds : Arrays.stream(connectIds.toString().split(",")).distinct().collect(Collectors.toList()));
        return new ArrayList<>(ids);
    }

    public String name(final String... subNames) {
        return nodeName(this.name, subNames);
    }

    public PipelineConstants.ENVIRONMENT environment() {
        if (isBlank(environment)) return AppConfig.environment();
        return PipelineConstants.ENVIRONMENT.environment(this.getEnvironment());
    }

    public static String nodeName(final String name, final String... subNames) {
        return String.format("%s:%s", name, String.join(":", subNames));
    }

    @Override
    public String toString() {
        return "Node{" + "name='" + name + '\'' + ", type='" + type + '\'' + '}';
    }
}
