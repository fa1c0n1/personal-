package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class Node implements Serializable {

    private static final long serialVersionUID = 1L;

    @BsonProperty(value = "schemaIds")
    private Set<String> schemaIds;

    private Map<String, String> schemaMapping = new HashMap<>();

    @BsonProperty(value = "nodeName")
    private String nodeName;

    @BsonProperty(value = "schemas")
    private Set<Schema> schemas;

    @BsonProperty(value = "stages")
    private Set<String> stages;

    @BsonProperty(value = "firstEventSeenOn")
    private Date firstEventSeenOn;

    @BsonProperty(value = "lastEventSeenOn")
    private Date lastEventSeenOn;

    public boolean updateNode(Node node) {

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
            this.stages.addAll(node.stages);
        } else {
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

            Map<String, Schema> map = convertToMap(this.schemas);

            for (Schema schema : node.getSchemas()) {
                if (map.containsKey(schema.getSchemaId())) {
                    map.get(schema.getSchemaId()).updateSchema(schema);
                } else {
                    schemas.add(schema);
                }
            }

        }

        return true;
    }

    private Map<String, Schema> convertToMap(Set<Schema> schemas) {
        Map<String, Schema> map = schemas.stream()
                .collect(Collectors.toMap(Schema::getSchemaId, Function.identity()));
        return map;
    }

}
