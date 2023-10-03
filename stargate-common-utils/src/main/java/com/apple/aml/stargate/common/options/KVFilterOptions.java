package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Set;
import java.util.regex.Pattern;

import static lombok.AccessLevel.NONE;

@Data
@EqualsAndHashCode(callSuper = true)
public class KVFilterOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Pattern keyRegexPattern;
    @Optional
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Pattern schemaIdRegexPattern;
    @Optional
    private String keyRegex;
    @Optional
    private String schemaIdRegex;
    @Optional
    private Set<String> schemaIds;
    @Optional
    private String keyStartsWith;
    @Optional
    private String keyEndsWith;
    @Optional
    private String keyContains;
    @Optional
    private String schemaIdStartsWith;
    @Optional
    private String schemaIdEndsWith;
    @Optional
    private String expression;
    @Optional
    private boolean separateFilters;
    @Optional
    private boolean negate;

    public void setKeyRegex(final String keyRegex) {
        if (keyRegex == null || keyRegex.isBlank()) {
            return;
        }
        keyRegexPattern = Pattern.compile(keyRegex);
    }

    public void setSchemaIdRegex(final String schemaIdRegex) {
        if (schemaIdRegex == null || schemaIdRegex.isBlank()) {
            return;
        }
        schemaIdRegexPattern = Pattern.compile(schemaIdRegex);
    }

    public boolean shouldOutput(final String recordKey, final String schemaId) {
        if (negate) {
            return (this.getSchemaId() == null || !schemaId.equals(this.getSchemaId())) && (this.getKeyContains() == null || !recordKey.contains(this.getKeyContains())) && (this.getKeyEndsWith() == null || !recordKey.endsWith(this.getKeyEndsWith())) && (this.getKeyStartsWith() == null || !recordKey.startsWith(this.getKeyStartsWith())) && (this.getSchemaIds() == null || !this.getSchemaIds().contains(schemaId)) && (this.getSchemaIdStartsWith() == null || !schemaId.startsWith(this.getSchemaIdStartsWith())) && (this.getSchemaIdEndsWith() == null || !schemaId.endsWith(this.getSchemaIdEndsWith())) && (keyRegexPattern == null || !keyRegexPattern.matcher(recordKey).find()) && (schemaIdRegexPattern == null || !schemaIdRegexPattern.matcher(schemaId).find());
        }
        return (this.getSchemaId() == null || schemaId.equals(this.getSchemaId())) && (this.getKeyContains() == null || recordKey.contains(this.getKeyContains())) && (this.getKeyEndsWith() == null || recordKey.endsWith(this.getKeyEndsWith())) && (this.getKeyStartsWith() == null || recordKey.startsWith(this.getKeyStartsWith())) && (this.getSchemaIds() == null || this.getSchemaIds().contains(schemaId)) && (this.getSchemaIdStartsWith() == null || schemaId.startsWith(this.getSchemaIdStartsWith())) && (this.getSchemaIdEndsWith() == null || schemaId.endsWith(this.getSchemaIdEndsWith())) && (keyRegexPattern == null || keyRegexPattern.matcher(recordKey).find()) && (schemaIdRegexPattern == null || schemaIdRegexPattern.matcher(schemaId).find());
    }
}
