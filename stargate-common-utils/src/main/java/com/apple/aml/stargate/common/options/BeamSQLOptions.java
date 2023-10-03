package com.apple.aml.stargate.common.options;


import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;

import static lombok.AccessLevel.NONE;

@Data
@EqualsAndHashCode(callSuper = true)
public class BeamSQLOptions extends DerivedSchemaOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private Object schemaIds;
    @Optional
    private Object sqlSchemas;
    @Optional
    private String sql;
    @Optional
    private List<String> sqls;
    @Optional
    private String keyAttribute;
    @Optional
    private boolean flatten = true;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private LinkedHashSet<String> _schemaIds;
    @Optional
    private boolean windowing;
    @Optional
    private Duration windowDuration = Duration.ofHours(1);
    @Optional
    private boolean triggering = true;
    @Optional
    private String windowAccumulationMode;
    @Optional
    private String triggerMode;
    @Optional
    private String windowClosingBehavior;
    @Optional
    private Duration windowLateness = Duration.ofSeconds(10);

    public void setSqlSchemas(final Object sqlSchemas) {
        this.sqlSchemas = sqlSchemas;
        if (sqlSchemas != null) setSchemaIds(sqlSchemas);
    }

    @SuppressWarnings("unchecked")
    public LinkedHashSet<String> schemaIds() {
        if (this._schemaIds != null) return this._schemaIds;
        this._schemaIds = schemaIds == null ? null : new LinkedHashSet<>(schemaIds instanceof List ? (List<String>) schemaIds : List.of(((String) schemaIds).split(",")));
        return this._schemaIds;
    }
}
