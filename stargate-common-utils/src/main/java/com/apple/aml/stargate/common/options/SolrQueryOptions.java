package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.apple.aml.stargate.common.constants.CommonConstants.SolrConstants.QUERY_ALL;

@Data
@EqualsAndHashCode(callSuper = true)
public class SolrQueryOptions extends SchemaMappingOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String className;
    @Optional
    private String collection;
    @Optional
    private String query;
    @Optional
    private String defaultQuery;
    @Optional
    private List<String> fields;
    @Optional
    private int start = -1;
    @Optional
    private int rows = 0;
    @Optional
    private Map<String, Object> params;
    @Optional
    private List<String> filterQueries;
    @Optional
    private List<String> sorts;
    @Optional
    private String keyExpression;

    public Set<String> queries() {
        Set<String> queries = new HashSet<>();
        if (defaultQuery != null && !defaultQuery.trim().isBlank()) {
            queries.add(defaultQuery.trim());
        }
        if (filterQueries != null && !filterQueries.isEmpty()) {
            queries.addAll(filterQueries);
        }
        if (query != null && !query.trim().isBlank()) {
            queries.add(query.trim());
        }
        queries.remove(QUERY_ALL);
        return queries;
    }
}
