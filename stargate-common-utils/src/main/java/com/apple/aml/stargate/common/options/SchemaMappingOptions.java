package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.PipelineConstants.DEFAULT_MAPPING;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static lombok.AccessLevel.NONE;

@Data
@EqualsAndHashCode(callSuper = true)
public class SchemaMappingOptions extends SchemaLevelOptions implements Serializable {
    @Optional
    @Setter(AccessLevel.NONE)
    private Map<String, Object> mappings;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Map<String, SchemaLevelOptions> schemaMappings;

    @SuppressWarnings("unchecked")
    public SchemaMappingOptions enableSchemaLevelDefaults() {
        if (this.mappings() == null) {
            this.setMappings(new HashMap<>());
        }
        Map<String, SchemaLevelOptions> newMappings = new HashMap<>();
        try {
            this.mappings().put(DEFAULT_MAPPING, duplicate(this, SchemaLevelOptions.class));
            Map mainMap = readJsonMap(jsonString(this));
            for (Map.Entry<String, SchemaLevelOptions> entry : this.schemaMappings.entrySet()) {
                Map options = new HashMap();
                options.putAll(mainMap);
                options.putAll(readJsonMap(jsonString(entry.getValue())));
                SchemaLevelOptions updatedOptions = getAs(options, SchemaLevelOptions.class);
                newMappings.put(entry.getKey(), updatedOptions);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        this.schemaMappings = newMappings;
        return this;
    }

    public Map<String, SchemaLevelOptions> mappings() {
        return this.schemaMappings;
    }

    public void setMappings(final Map<String, Object> mappings) {
        this.mappings = mappings;
        if (this.mappings == null) {
            return;
        }
        schemaMappings = new HashMap<>();
        try {
            for (Map.Entry<String, Object> entry : this.mappings.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }
                SchemaLevelOptions options = getAs(entry.getValue(), SchemaLevelOptions.class);
                this.schemaMappings.put(entry.getKey(), options);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
