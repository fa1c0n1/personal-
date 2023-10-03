package com.apple.aml.stargate.common.pojo;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.SchemaUtils.TYPE_ALIASES;
import static java.util.Arrays.asList;

@Data
@JsonPropertyOrder({"fieldName", "fieldType", "mandatory", "documentation", "defaultValue"})
public class SimpleSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    private String fieldName;
    @Optional
    private String fieldType;
    @Optional
    private boolean mandatory = true;
    @Optional
    private String documentation;
    @Optional
    private Object defaultValue;

    public Map<String, Object> avroSpec() {
        String fieldType = (this.fieldType == null ? "string" : this.fieldType).toLowerCase().trim();
        Object type = TYPE_ALIASES.getOrDefault(fieldType, fieldType);
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", fieldName.trim());
        map.put("type", mandatory ? type : asList(type, "null"));
        if (!(documentation == null || documentation.isBlank())) {
            map.put("doc", documentation.trim());
        }
        if (defaultValue != null && !String.valueOf(defaultValue).isBlank()) {
            map.put("default", defaultValue);
        }
        return map;
    }
}
