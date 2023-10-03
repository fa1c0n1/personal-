package com.apple.aml.stargate.beam.sdk.io.http;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JdbcIndexOptions;
import com.apple.aml.stargate.common.options.LocalOfsOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.io.http.LocalOfsInvoker.ofsRelationalTableName;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchPartitionKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_GROUP_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.RM;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_KV;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_RELATIONAL;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_LOCAL_RM_URI;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class LocalOfsIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final Map<String, Set<JdbcIndexOptions>> tableIndexes = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return transformCollection(pipeline, node, collection, false);
    }

    public SCollection<KV<String, GenericRecord>> transformCollection(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection, final boolean emit) throws Exception {
        LocalOfsOptions options = (LocalOfsOptions) node.getConfig();
        if (isBlank(options.getBaseUri())) {
            options.setBaseUri(env(STARGATE_LOCAL_RM_URI, String.format("http://localhost:%d", config().hasPath("server.rm.port") ? config().getInt("server.rm.port") : RM)));
        }
        if (isBlank(options.getCacheName())) {
            options.setCacheName("kv".equalsIgnoreCase(options.getType()) ? CACHE_NAME_KV : CACHE_NAME_RELATIONAL);
        }

        buildJdbcIndexOptions(options, node);
        PipelineConstants.ENVIRONMENT environment = node.environment();
        SCollection<KV<String, GenericRecord>> window = applyWindow(collection, options, node.getName());
        if (options.getBatchSize() > 1) {
            LOGGER.debug("Optimized batching enabled. Will apply batching", Map.of("batchSize", options.getBatchSize(), "partitions", options.getPartitions()));
            return window.group(node.name("partition-key"), batchPartitionKey(options.getPartitions(), options.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("batch"), batchWriter(node.getName(), node.getType(), environment, options, new LocalOfsWriter<>(node, options, tableIndexes, emit)));
        } else {
            return window.apply(node.getName(), new LocalOfsInvoker(node, options, tableIndexes, emit));
        }
    }

    private void buildJdbcIndexOptions(final LocalOfsOptions options, final StargateNode node) {
        if (options.getIndexes() == null || options.getIndexes().isEmpty()) return;

        String tableNameTemplateName = options.getTableName() == null ? null : (node.getName() + "~tableName");
        if (options.getTableName() != null) {
            loadFreemarkerTemplate(tableNameTemplateName, options.getTableName());
        }
        Configuration configuration = freeMarkerConfiguration();
        options.getIndexes().forEach((schemaId, v) -> {
            Schema schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), schemaId);
            String tableName = ofsRelationalTableName(schema, node.getName(), node.getType(), configuration, tableNameTemplateName);
            Set<JdbcIndexOptions> indexes = tableIndexes.computeIfAbsent(tableName, tblName -> new HashSet<>());
            if (v instanceof String) {
                JdbcIndexOptions index = new JdbcIndexOptions();
                index.setName(String.format("idx_%s_%d", tableName, 1));
                index.setColumns(Arrays.asList(((String) v).split(DEFAULT_DELIMITER)));
                indexes.add(index);
            } else if (v instanceof List) {
                int indexNo = 0;
                for (Object o : (List) v) {
                    indexNo++;
                    JdbcIndexOptions index;
                    if (o instanceof String) {
                        try {
                            index = readJson((String) o, JdbcIndexOptions.class);
                        } catch (Exception ignored) {
                            index = new JdbcIndexOptions();
                            index.setColumns(Arrays.asList(((String) o).split(DEFAULT_DELIMITER)));
                        }
                    } else {
                        try {
                            index = getAs(o, JdbcIndexOptions.class);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (index == null) {
                        throw new RuntimeException("Not a valid json String");
                    }
                    if (isBlank(index.getName())) {
                        index.setName(String.format("idx_%s_%d", tableName, indexNo));
                    }
                    indexes.add(index);
                }
            } else {
                JdbcIndexOptions index;
                try {
                    index = getAs(v, JdbcIndexOptions.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (index == null) {
                    throw new RuntimeException("Not a valid json String");
                }
                if (isBlank(index.getName())) {
                    index.setName(String.format("idx_%s_%d", tableName, 1));
                }
                indexes.add(index);
            }
        });
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return transformCollection(pipeline, node, collection, true);
    }
}
