package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.stargate.cache.utils.JdbcUtils;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.DMLOperation;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.apple.aml.stargate.cache.utils.CacheMetrics.CREATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.READ_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.UPDATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.JdbcUtils.fetchSQLResults;
import static com.apple.aml.stargate.common.constants.CommonConstants.MDCKeys.SOURCE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_PROVIDER_IGNITE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_PROVIDER_JDBC;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_RELATIONAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.EXCEPTION;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OK;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OVERALL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.As;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastReadJson;
import static com.apple.aml.stargate.common.utils.LogUtils.getFromMDCOrDefault;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.schema;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-jdbc")
@Lazy
public class IgniteJdbcCacheService extends IgniteLocalCacheService implements CacheService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final Pattern createTablePattern = Pattern.compile("(create table)( if not exists)? ([^(]+)", Pattern.CASE_INSENSITIVE);
    private final Pattern createIndexPattern = Pattern.compile("(create\\s+index)(\\s+if\\s+not\\s+exists\\s+)?(\\w+)(\\s+on\\s+)(\\w+)(\\s+.*)?", Pattern.CASE_INSENSITIVE);
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    @Qualifier("ignite-jdbc-template")
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Value("${stargate.cache.ofs.ignite-jdbc.datasource.currentSchema}")
    private String defaultSchema;

    @PostConstruct
    public void init() {
        super.init();
        if (defaultSchema != null) {
            defaultSchema = defaultSchema.trim();
        }
    }

    public Mono<Boolean> executeDDL(final String cacheName, final String ddl) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                sink.success(JdbcUtils.executeDML(jdbcTemplate, ddl));
                CREATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDDL", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                sink.success(true);
            } catch (Exception e) {
                if (String.valueOf(e.getMessage()).startsWith("Table already exists")) { // very bad way; will fix it using IgniteSQLException
                    LOGGER.warn("Table already exists. Will ignore", Map.of("cacheName", cacheName, "sql", ddl), e);
                    CREATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDDL", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                    sink.success(true);
                } else {
                    CREATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDDL", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                    sink.error(e);
                }
            }
        });
    }

    @Override
    public Mono<Boolean> createTableDDL(final String cacheName, final String tableName, final String ddl) {
        String sql = ddl;
        if (!defaultSchema.isBlank()) {
            Matcher matcher = createTablePattern.matcher(ddl);
            if (matcher.find()) {
                String updatedTableName = String.format("%s.%s", defaultSchema, matcher.group(3).trim());
                sql = matcher.replaceFirst("$1$2 " + updatedTableName);
            }
        }
        return executeDDL(cacheName, sql);
    }

    public Mono<Boolean> createIndexDDL(final String cacheName, final String tableName, final String indexName, final String ddl) {
        String sql = ddl;
        if (!defaultSchema.isBlank()) {
            Matcher matcher = createIndexPattern.matcher(ddl);
            if (matcher.find()) {
                String updatedTableName = String.format("%s.%s", defaultSchema, matcher.group(5).trim());
                sql = matcher.replaceFirst("$1$2$3$4" + updatedTableName + "$6");
            }
        }
        return executeDDL(cacheName, sql);
    }

    public Mono<Boolean> executeDML(final String cacheName, final String dml) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                sink.success(JdbcUtils.executeDML(jdbcTemplate, dml));
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDML", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDML", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing query", Map.of("cacheName", cacheName, "sql", dml), e);
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, Boolean>> executeDMLs(final String cacheName, final List<String> dmls) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            String dml = null;
            try {
                Map<String, Boolean> returnMap = new HashMap<>();
                for (String sql : dmls) {
                    dml = sql;
                    long singleStartTime = System.nanoTime();
                    returnMap.put(dml, JdbcUtils.executeDML(jdbcTemplate, dml));
                    UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDML", OVERALL, SUCCESS, OK).observe((System.nanoTime() - singleStartTime) / 1000000.0);
                }
                sink.success(returnMap);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDMLs", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeDMLs", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "dmls", dmls, "currentSQL", String.valueOf(dml)), e);
                sink.error(e);
            }
        });
    }

    public Mono<List<Map>> executeQuery(final String cacheName, final String sql) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                List<Map> returnList = fetchSQLResults(jdbcTemplate, sql);
                sink.success(returnList);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeQuery", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeQuery", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing query", Map.of("cacheName", cacheName, "sql", sql), e);
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, List<Map>>> executeQueries(final String cacheName, final List<String> sqls) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                Map<String, List<Map>> returnMap = new HashMap<>();
                for (final String sql : sqls) {
                    long singleStartTime = System.nanoTime();
                    List<Map> returnList = fetchSQLResults(jdbcTemplate, sql);
                    returnMap.put(sql, returnList);
                    READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeQuery", OVERALL, SUCCESS, OK).observe((System.nanoTime() - singleStartTime) / 1000000.0);
                }
                sink.success(returnMap);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeQueries", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "executeQueries", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "sqls", sqls), e);
                sink.error(e);
            }
        });
    }

    @Override
    public Mono<Map<String, Boolean>> executePOJOs(final String cacheName, final List<Object> pojos) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            String dml = null;
            try {
                Map<String, Boolean> returnMap = new HashMap<>();
                String[] dmls = new String[pojos.size()];
                int i = 0;
                for (Object object : pojos) {
                    DMLOperation operation = (object instanceof String) ? fastReadJson((String) object, DMLOperation.class) : As(object, DMLOperation.class);
                    if (operation.getSchemaId() == null) {
                        dml = operation.postgresql();
                    } else {
                        String schemaId = operation.getSchemaId();
                        ObjectToGenericRecordConverter converter = converterMap.computeIfAbsent(schemaId, s -> {
                            try {
                                return converter(schema(operation.getSchemaReference(), schemaId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                        GenericRecord record = converter.convert(operation.getData());
                        dml = operation.postgresql(record);
                    }
                    dmls[i++] = dml;
                }
                int[] results = jdbcTemplate.batchUpdate(dmls);
                for (i = 0; i < results.length; i++) {
                    returnMap.put(dmls[i], results[i] > 0);
                }
                sink.success(returnMap);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executePOJOs", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executePOJOs", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "pojos", pojos, "currentSQL", String.valueOf(dml)), e);
                sink.error(e);
            }
        });
    }
}
