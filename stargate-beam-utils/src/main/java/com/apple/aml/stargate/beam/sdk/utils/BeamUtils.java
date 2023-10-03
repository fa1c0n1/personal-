package com.apple.aml.stargate.beam.sdk.utils;

import com.apple.aml.stargate.beam.inject.BeamErrorHandler;
import com.apple.aml.stargate.beam.inject.BeamNodeServiceHandler;
import com.apple.aml.stargate.beam.sdk.coders.AvroCoder;
import com.apple.aml.stargate.beam.sdk.coders.ErrorRecordCoder;
import com.apple.aml.stargate.beam.sdk.coders.FstCoder;
import com.apple.aml.stargate.beam.sdk.coders.KafkaMessageCoder;
import com.apple.aml.stargate.beam.sdk.coders.RawMessageCoder;
import com.apple.aml.stargate.beam.sdk.io.kafka.KafkaMessage;
import com.apple.aml.stargate.beam.sdk.io.kafka.RawMessage;
import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.beam.sdk.predicates.FreemarkerPredicate;
import com.apple.aml.stargate.beam.sdk.predicates.KVFilterPredicate;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.options.KVFilterOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.pojo.ErrorPayload;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import com.google.inject.Injector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.kryo.KryoCoderProvider;
import org.apache.beam.sdk.extensions.kryo.KryoRegistrar;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;

import static com.apple.aml.stargate.beam.sdk.ts.PredicateFilter.filterBy;
import static com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterKey;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.schemaReference;

public final class BeamUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final NodeService nodeService = new BeamNodeServiceHandler();
    private static final ErrorService errorService = new BeamErrorHandler();

    private BeamUtils() {
    }


    @SuppressWarnings("unchecked")
    public static AvroCoder registerDefaultCoders(final Pipeline pipeline, final String schemaReference, final PipelineConstants.ENVIRONMENT environment) {
        AvroCoder coder = AvroCoder.of(schemaReference == null ? schemaReference(environment) : schemaReference);
        ErrorRecordCoder errorRecordCoder = ErrorRecordCoder.of(coder);
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<GenericRecord>() {
        }, coder));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<AvroRecord>() {
        }, coder));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<GenericData.Record>() {
        }, coder));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<ErrorRecord>() {
        }, errorRecordCoder));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<Exception>() {
        }, SerializableCoder.of(Exception.class)));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<Throwable>() {
        }, SerializableCoder.of(Throwable.class)));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<GenericException>() {
        }, SerializableCoder.of(GenericException.class)));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<KV<String, ErrorRecord>>() {
        }, KvCoder.of(StringUtf8Coder.of(), errorRecordCoder)));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<ErrorPayload>() {
        }, SerializableCoder.of(ErrorPayload.class)));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<KV<String, ErrorPayload>>() {
        }, KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ErrorPayload.class))));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<WriterKey>() {
        }, SerializableCoder.of(WriterKey.class)));
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<SolrDocument>() {
        }, SerializableCoder.of(SolrDocument.class)));

        StargateOptions options = pipeline.getOptions().as(StargateOptions.class);
        switch (options.getCustomSerializer()) {
            case fst:
                coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<RawMessage>() {
                }, FstCoder.of(RawMessage.class)));
                coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<KafkaMessage>() {
                }, FstCoder.of(KafkaMessage.class)));
                break;
            case kryo:
                KryoRegistrar kryoRegistrar = k -> {
                    k.register(KafkaMessage.class);
                    k.register(RawMessage.class);
                };
                KryoCoderProvider kryoCoderProvider = KryoCoderProvider.of(pipeline.getOptions(), kryoRegistrar);
                coderRegistry.registerCoderProvider(kryoCoderProvider);
                break;
            default:
                coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<RawMessage>() {
                }, RawMessageCoder.of()));
                coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<KafkaMessage>() {
                }, KafkaMessageCoder.of()));
        }
        return coder;
    }

    public static SCollection<KV<String, GenericRecord>> applyKVFilters(final String nodeName, final String nodeType, final KVFilterOptions options, final SCollection<KV<String, GenericRecord>> inputCollection) {
        if (options == null) return inputCollection;
        SCollection<KV<String, GenericRecord>> collection = inputCollection;
        if (options.isSeparateFilters() && !options.isNegate()) {
            if (options.getKeyRegex() != null && !options.getKeyRegex().isBlank()) {
                final Pattern regexPattern = Pattern.compile(options.getKeyRegex());
                LOGGER.debug("keyRegex filter enabled. Will create filter transformation at", Map.of("pattern", regexPattern, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "key-regex"), filterBy(kv -> regexPattern.matcher(kv.getKey()).find(), nodeName(nodeName, "key-regex"), nodeType));
            }
            if (options.getKeyStartsWith() != null && !options.getKeyStartsWith().isBlank()) {
                final String startsWith = options.getKeyStartsWith().trim();
                LOGGER.debug("keyStartsWith filter enabled. Will create filter transformation at", Map.of("startsWith", startsWith, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "key-starts-with"), filterBy(kv -> kv.getKey().startsWith(startsWith), nodeName(nodeName, "key-starts-with"), nodeType));
            }
            if (options.getKeyEndsWith() != null && !options.getKeyEndsWith().isBlank()) {
                final String endsWith = options.getKeyEndsWith().trim();
                LOGGER.debug("keyEndsWith filter enabled. Will create filter transformation at", Map.of("endsWith", endsWith, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "key-ends-with"), filterBy(kv -> kv.getKey().endsWith(endsWith), nodeName(nodeName, "key-ends-with"), nodeType));
            }
            if (options.getKeyContains() != null && !options.getKeyContains().isBlank()) {
                final String contains = options.getKeyContains().trim();
                LOGGER.debug("keyContains filter enabled. Will create filter transformation at", Map.of("contains", contains, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "key-contains"), filterBy(kv -> kv.getKey().contains(contains), nodeName(nodeName, "key-contains"), nodeType));
            }
            if (options.getSchemaIdStartsWith() != null && !options.getSchemaIdStartsWith().isBlank()) {
                final String startsWith = options.getSchemaIdStartsWith().trim();
                LOGGER.debug("schemaIdStartsWith filter enabled. Will create filter transformation at", Map.of("startsWith", startsWith, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "schemaId-starts-with"), filterBy(kv -> {
                    GenericRecord record = kv.getValue();
                    if (record == null) {
                        return false;
                    }
                    return record.getSchema().getFullName().startsWith(startsWith);
                }, nodeName(nodeName, "schemaId-starts-with"), nodeType));
            }
            if (options.getSchemaIdEndsWith() != null && !options.getSchemaIdEndsWith().isBlank()) {
                final String endsWith = options.getSchemaIdEndsWith().trim();
                LOGGER.debug("schemaIdEndsWith filter enabled. Will create filter transformation at", Map.of("endsWith", endsWith, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "schemaId-ends-with"), filterBy(kv -> {
                    GenericRecord record = kv.getValue();
                    if (record == null) {
                        return false;
                    }
                    return record.getSchema().getFullName().endsWith(endsWith);
                }, nodeName(nodeName, "schemaId-ends-with"), nodeType));
            }
            if (options.getSchemaIdRegex() != null && !options.getSchemaIdRegex().isBlank()) {
                final Pattern regexPattern = Pattern.compile(options.getSchemaIdRegex());
                LOGGER.debug("schemaIdRegex filter enabled. Will create filter transformation at", Map.of("pattern", regexPattern, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "schemaId-regex"), filterBy(kv -> {
                    GenericRecord record = kv.getValue();
                    if (record == null) {
                        return false;
                    }
                    Schema schema = record.getSchema();
                    String schemaId = schema.getFullName();
                    return regexPattern.matcher(schemaId).find();
                }, nodeName(nodeName, "schemaId-regex"), nodeType));
            }
            if (options.getSchemaId() != null && !options.getSchemaId().isBlank()) {
                final String filterSchemaId = options.getSchemaId().trim();
                LOGGER.debug("schemaId filter enabled. Will create filter transformation at", Map.of(SCHEMA_ID, filterSchemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "schemaId"), filterBy(kv -> {
                    GenericRecord record = kv.getValue();
                    if (record == null) {
                        return false;
                    }
                    return filterSchemaId.equals(record.getSchema().getFullName());
                }, nodeName(nodeName, "schemaId"), nodeType));
            }
            if (options.getSchemaIds() != null && !options.getSchemaIds().isEmpty()) {
                final HashSet<String> filterSchemaIds = new HashSet<>(options.getSchemaIds());
                LOGGER.debug("schemaIds filter enabled. Will create filter transformation at", Map.of("schemaIds", filterSchemaIds, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                collection = collection.apply(nodeName(nodeName, "schemaIds"), filterBy(kv -> {
                    GenericRecord record = kv.getValue();
                    if (record == null) {
                        return false;
                    }
                    return filterSchemaIds.contains(record.getSchema().getFullName());
                }, nodeName(nodeName, "schemaIds"), nodeType));
            }
            if (options.getExpression() != null && !options.getExpression().isBlank()) {
                String expression = options.getExpression().trim();
                LOGGER.debug("expression filter enabled. Will create freemarker expression filter using", Map.of("expression", expression, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                String filterNodeName = nodeName(nodeName, "expression");
                collection = collection.apply(filterNodeName, filterBy(new FreemarkerPredicate(expression, filterNodeName), filterNodeName, nodeType));
            }
        } else {
            LOGGER.debug("Merged filter enabled. Will create filter transformation at", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
            collection = collection.apply(nodeName(nodeName, "merged"), filterBy(new KVFilterPredicate(options, nodeName(nodeName, "merged"), nodeType), nodeName, nodeType));
        }
        return collection;
    }

    public static NodeService nodeService() {
        return nodeService;
    }

    public static ErrorService errorService() {
        return errorService;
    }

    public static Injector getInjector(final String nodeName, final JavaFunctionOptions options) throws Exception {
        return PipelineUtils.getInjector(nodeName, options, nodeService(), errorService());
    }

}
