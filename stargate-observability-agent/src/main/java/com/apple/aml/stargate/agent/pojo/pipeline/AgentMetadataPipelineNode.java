package com.apple.aml.stargate.agent.pojo.pipeline;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class AgentMetadataPipelineNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private Set<String> schemaIds = new HashSet<>();

    private String nodeName;

    private Map<String, String> schemaMapping = new HashMap<>();

    private Set<AgentMetadataSchema> schemas = new HashSet<>();

    private Set<String> stages = new HashSet<>();

    private Date firstEventSeenOn;

    private Date lastEventSeenOn;

    public boolean updateNode(AgentMetadataPipelineNode node) {

        boolean isChanged = false;

        if (this.nodeName != null && !this.nodeName.equals(node.nodeName)) {
            return false;
        }

        if (this.schemaIds != null) {
            this.schemaIds.addAll(node.schemaIds);
        } else {
            this.schemaIds = node.schemaIds;
        }

        if (this.schemaMapping != null) {
            this.schemaMapping.putAll(node.getSchemaMapping());
        } else {
            this.schemaMapping = node.getSchemaMapping();
        }

        if (this.stages != null) {
            int beforeChange = this.stages.size();
            this.stages.addAll(node.stages);
            if (beforeChange != stages.size()) {
                isChanged = true;
            }
        } else {
            isChanged = true;
            this.stages = node.stages;
        }

        if (this.firstEventSeenOn == null || (node.firstEventSeenOn != null && this.firstEventSeenOn.after(node.firstEventSeenOn))) {
            this.firstEventSeenOn = node.firstEventSeenOn;
        }

        if (this.lastEventSeenOn == null || (node.lastEventSeenOn != null && this.lastEventSeenOn.before(node.lastEventSeenOn))) {
            this.lastEventSeenOn = node.lastEventSeenOn;
        }

        if (this.schemas == null) {
            this.schemas = node.getSchemas();
        } else if (node.getSchemas() != null) {

            Map<String, AgentMetadataSchema> map = convertToMap(this.schemas);

            for (AgentMetadataSchema schema : node.getSchemas()) {
                if (map.containsKey(schema.getSchemaId())) {
                    map.get(schema.getSchemaId()).updateSchema(schema);
                } else {
                    schemas.add(schema);
                }
            }

        }

        return isChanged;
    }

    private Map<String, AgentMetadataSchema> convertToMap(Set<AgentMetadataSchema> schemas) {
        Map<String, AgentMetadataSchema> map = schemas.stream()
                .collect(Collectors.toMap(AgentMetadataSchema::getSchemaId, Function.identity()));
        return map;
    }

}
