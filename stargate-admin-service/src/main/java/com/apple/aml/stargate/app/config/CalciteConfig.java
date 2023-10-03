package com.apple.aml.stargate.app.config;

import com.apple.aml.stargate.app.pojo.MongoCalciteView;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.calcite.adapter.mongodb.MongoDbSchemaFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.mode;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconToPojo;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.SchemaUtils.schema;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

@Configuration
public class CalciteConfig {
    public static final String SCHEMA_NAME_RAW_MONGO = "stargate_mongo";
    public static final String SCHEMA_NAME_DEFAULT = "stargate";
    private static final String FACTORY_CLASS = MongoDbSchemaFactory.class.getCanonicalName();
    private static Pattern NORMALIZE_TOKEN_PATTERN = Pattern.compile("\\#\\{([^}]+)\\}", CASE_INSENSITIVE);

    @Autowired
    private MongoDatabase mongoDatabase;

    private static String getInlineModel() throws Exception {
        List<Map<String, Object>> schemas = new ArrayList<>();
        List<Map<String, Object>> views = new ArrayList<>();
        schemas.add(Map.of("type", "custom", "name", SCHEMA_NAME_RAW_MONGO, "factory", FACTORY_CLASS, "operand", Map.of()));
        schemas.add(Map.of("name", SCHEMA_NAME_DEFAULT, "tables", views));
        Pattern pattern = Pattern.compile("\\#\\{([^}]+)\\}", CASE_INSENSITIVE);
        Map<String, String> replacements = Map.of("ENV", mode().toLowerCase(), "environment", mode());
        for (Config config : config().getConfigList("stargate.calcite.views")) {
            MongoCalciteView details = hoconToPojo(normalizeString(config.root().render(), replacements), MongoCalciteView.class);
            Schema schema = schema(details.getSchemaId());
            String columnsSQL = schema.getFields().stream().map(f -> String.format("cast(_MAP['%s'] AS %s) as \"%s\"", f.name(), calciteType(f.schema()).name().toLowerCase(), f.name())).collect(Collectors.joining(", "));
            String sql = String.format("select %s from \"%s\".\"%s\" %s", columnsSQL, SCHEMA_NAME_RAW_MONGO, details.getCollectionName(), details.getAppendClause() == null ? EMPTY_STRING : details.getAppendClause());
            views.add(Map.of("name", details.getViewName(), "type", "view", "sql", sql));
        }
        return "inline:" + jsonString(Map.of("version", "1.0", "defaultSchema", SCHEMA_NAME_DEFAULT, "schemas", schemas));
    }

    private static String normalizeString(final String input, final Map<String, String> properties) {
        Set<String> inputTokens = new HashSet<>();
        inputTokens.addAll(NORMALIZE_TOKEN_PATTERN.matcher(input).results().map(result -> result.group(1)).collect(Collectors.toList()));
        Map<String, String> tokenValues = new HashMap<>();
        for (final String token : inputTokens) {
            String value = properties.get(token);
            if (value != null) {
                tokenValues.put(token, value);
                continue;
            }
            value = System.getProperty(token);
            if (value != null) {
                tokenValues.put(token, value);
                continue;
            }
            value = System.getenv(token);
            tokenValues.put(token, value == null ? "#{" + token + "}" : value);
        }
        String content = input;
        for (Map.Entry<String, String> tokenEntry : tokenValues.entrySet()) {
            content = content.replaceAll("\\#\\{" + tokenEntry.getKey() + "\\}", tokenEntry.getValue());
        }
        return content;
    }

    private static SqlTypeName calciteType(final Schema schema) {
        switch (schema.getType()) {
            case UNION:
                List<Schema> types = schema.getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return calciteType(type);
                }
            case INT:
                return SqlTypeName.INTEGER;
            case LONG:
                return SqlTypeName.BIGINT;
            case FLOAT:
                return SqlTypeName.FLOAT;
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case BYTES:
                return SqlTypeName.BINARY;
            case BOOLEAN:
                return SqlTypeName.BOOLEAN;
            default:
                return SqlTypeName.VARCHAR;
        }
    }

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties dataSourceProperties() throws Exception {
        MongoDbSchemaFactory.INSTANCE = new MongoDbSchemaFactory(mongoDatabase);
        DataSourceProperties properties = new DataSourceProperties();
        properties.setName("stargate-calcite");
        properties.setUrl(String.format("jdbc:calcite:caseSensitive=false; model=%s", getInlineModel()));
        return properties;
    }
}
