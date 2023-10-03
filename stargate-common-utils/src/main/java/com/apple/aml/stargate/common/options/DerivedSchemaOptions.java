package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import static com.apple.aml.stargate.common.utils.AppConfig.mode;
import static com.apple.aml.stargate.common.utils.CsvUtils.readSimpleSchema;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static lombok.AccessLevel.NONE;

@Data
@EqualsAndHashCode(callSuper = true)
public class DerivedSchemaOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String schemaType;
    @Optional
    private Object schema;
    @Optional
    private Object appendSchema;
    @Optional
    private Object replaceSchema;
    @Optional
    private Object schemaOverride;
    @Optional
    private Object schemaIncludes;
    @Optional
    private Object schemaExcludes;
    @Optional
    private String schemaRegex;
    @Optional
    private boolean enableSimpleSchema;
    @Optional
    private boolean enableHierarchicalSchemaFilters;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private String _schemaOverride;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Set<String> _schemaIncludes;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Set<String> _schemaExcludes;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Pattern _schemaRegex;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private boolean _deriveSchema;

    public void setAppendSchema(final Object appendSchema) {
        this.schema = appendSchema;
        this.schemaType = "append";
    }

    public void setReplaceSchema(final Object replaceSchema) {
        this.schema = replaceSchema;
        this.schemaType = "replace";
    }

    public void setSchemaOverride(final Object replaceSchema) {
        setReplaceSchema(replaceSchema);
    }

    @SuppressWarnings("unchecked")
    private Set<String> _schemaIncludes() {
        return this.schemaIncludes == null ? null : new HashSet<>(schemaIncludes instanceof List ? (List<String>) schemaIncludes : List.of(((String) schemaIncludes).split(",")));
    }

    @SuppressWarnings("unchecked")
    private Set<String> _schemaExcludes() {
        return this.schemaExcludes == null ? null : new HashSet<>(schemaExcludes instanceof List ? (List<String>) schemaExcludes : List.of(((String) schemaExcludes).split(",")));
    }

    public void initSchemaDeriveOptions() {
        if (this.getSchema() != null && (isBlank(this.getSchemaType()) || "append".equalsIgnoreCase(this.getSchemaType()) || "merge".equalsIgnoreCase(this.getSchemaType()) || "enhance".equalsIgnoreCase(this.getSchemaType()))) {
            String schemaString;
            if (this.getSchema() instanceof String) {
                String objString = ((String) this.getSchema()).trim();
                if (objString.startsWith("{")) schemaString = objString;
                else schemaString = readSimpleSchema(String.format("com.apple.stargate.adhoc.%s.Random%d", mode(), Math.abs((new Random()).nextInt())), objString);
            } else {
                schemaString = jsonString(this.getSchema());
            }
            this._schemaOverride = new Schema.Parser().parse(schemaString).toString();
        }
        this._schemaIncludes = this._schemaIncludes();
        this._schemaExcludes = this._schemaExcludes();
        this._schemaRegex = isBlank(this.getSchemaRegex()) ? null : Pattern.compile(this.getSchemaRegex().trim());
        this._deriveSchema = this._schemaOverride != null || this._schemaIncludes != null || this._schemaExcludes != null || this._schemaRegex != null;
    }

    public Set<String> schemaIncludes() {
        return this._schemaIncludes;
    }

    public Set<String> schemaExcludes() {
        return this._schemaExcludes;
    }

    public Pattern schemaRegex() {
        return this._schemaRegex;
    }

    public String schemaOverride() {
        return this._schemaOverride;
    }

    public boolean deriveSchema() {
        return this._deriveSchema;
    }


}
