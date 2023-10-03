package com.apple.aml.stargate.common.nodes;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static lombok.AccessLevel.NONE;

@Data
@EqualsAndHashCode(callSuper = true)
public class StargatePipeline extends StargateNode<Map<String, Object>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Optional
    private String author;
    @Optional
    private Object dag;
    @Optional
    private List<Object> flows;
    @Optional
    private Map<String, Object> jobs;
    @Optional
    private Map<String, Object> jobargs;
    @Optional
    private Object jobGraph;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Map<String, StargateNode> nodes;
    @Optional
    private boolean dynamicJobName;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Map<String, StargateNode> aliases;
    @Optional
    private Map<String, Object> schemas;

    public Map<String, StargateNode> nodes() {
        return this.nodes;
    }

    public Map<String, StargateNode> nodes(final Map<String, StargateNode> nodes) {
        this.nodes = nodes;
        return this.nodes;
    }

    public void enableDirectAccess() {
        this.aliases = new HashMap<>(this.nodes);
        for (StargateNode node : nodes.values()) {
            if (!isBlank(node.getAlias())) Arrays.stream(node.getAlias().split(",")).distinct().forEach(k -> aliases.put(k, node));
            this.aliases.put(node.getName().toLowerCase(), node);
            this.aliases.put(node.getName().toUpperCase(), node);
            this.aliases.put(node.getName().toLowerCase().replaceAll("~", ":"), node);
            this.aliases.put(node.getName().toUpperCase().replaceAll("~", ":"), node);
            this.aliases.put(node.getName().toLowerCase().replaceAll(":", "~"), node);
            this.aliases.put(node.getName().toUpperCase().replaceAll(":", "~"), node);
        }
    }

    public StargateNode node(final String name) {
        return this.aliases.get(name.toLowerCase());
    }

    public Map<String, StargateNode> getNodes() {
        return new TreeMap<>(nodes);
    }
}
