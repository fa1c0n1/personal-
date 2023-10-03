package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.beam.sdk.io.kafka.KafkaMessage;
import com.apple.aml.stargate.beam.sdk.io.kafka.RawDeserializer;
import com.apple.aml.stargate.beam.sdk.io.kafka.RawMessage;
import com.apple.aml.stargate.beam.sdk.io.splunk.LogbackWriter;
import com.apple.aml.stargate.beam.sdk.transforms.ByteArrayToGenericRecord;
import com.apple.aml.stargate.beam.sdk.transforms.ObjectToGenericRecord;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.KafkaConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import com.apple.aml.stargate.common.constants.PipelineConstants.ENVIRONMENT;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.ACIKafkaOptions;
import com.apple.aml.stargate.common.options.BeamSQLOptions;
import com.apple.aml.stargate.common.options.SplunkOptions;
import com.apple.aml.stargate.common.utils.AMPSchemaStore;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.KafkaNoOpMetricsReporter;
import com.apple.aml.stargate.common.utils.KafkaPrometheusMetricSyncer;
import com.apple.amp.schemastore.model.SchemaStoreEndpoint;
import com.apple.pie.queue.client.ext.schema.avro.schema.resolver.UseWriteSchema;
import com.apple.pie.queue.client.ext.schema.header.SchemaHeaderSerDe;
import com.apple.pie.queue.kafka.client.configinterceptors.ConsumerConfigurationInterceptor;
import com.apple.pie.queue.kafka.client.configinterceptors.ConsumerGroupIdConfigurationInterceptor;
import com.apple.pie.queue.kafka.client.configinterceptors.DevModeConfigurationInterceptor;
import com.apple.pie.queue.kafka.client.configinterceptors.KaffeConfigurationInterceptor;
import com.apple.pie.queue.kafka.client.configinterceptors.ProducerConfigurationInterceptor;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.transforms.UtilFns.ensureNonNullableKey;
import static com.apple.aml.stargate.beam.sdk.ts.BeamSQL.applyTransform;
import static com.apple.aml.stargate.beam.sdk.ts.PredicateFilter.filterBy;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.KV_STRING_VS_STRING_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.DOT;
import static com.apple.aml.stargate.common.constants.CommonConstants.FORMATTER_DATE_TIME_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.REDACTED_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_SCHEMA_REFERENCE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.utils.ACIPrivateKeyProvider.setPrivateKeyProvider;
import static com.apple.aml.stargate.common.utils.AppConfig.appName;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.FormatUtils.parseDateTime;
import static com.apple.aml.stargate.common.utils.JsonUtils.isRedactable;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.appleSchemaStoreEndpoint;
import static com.apple.aml.stargate.common.utils.SchemaUtils.schemaReference;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.apple.pie.queue.client.ext.schema.avro.KafkaAvroCommonConfig.SCHEMA_STORE_CLASS_CONFIG;
import static com.apple.pie.queue.client.ext.schema.avro.KafkaAvroDeserializerConfig.READ_SCHEMA_RESOLVE_FACTORY_CLASS_CONFIG;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_CLIENT_SERVICE_NAME_CONFIG;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_HOST_CONFIG;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_PORT_CONFIG;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_PROTOCOL_CONFIG;
import static com.apple.pie.queue.kafka.client.KafkaClientUtils.DEV_MODE_ENABLED;
import static com.apple.pie.queue.kafka.client.configinterceptors.ClientConfigurationConfigDef.KAFFE_DEFAULTS_INJECTION;
import static com.apple.pie.queue.kafka.client.kaffe.KaffeConfig.KAFFE_CLIENT_ID_CONFIG;
import static com.apple.pie.queue.kafka.client.kaffe.KaffeConfig.KAFFE_CONNECT_CONFIG;
import static com.apple.pie.queue.kafka.client.kaffe.KaffeConfig.KAFFE_NAMESPACE_ID_CONFIG;
import static com.apple.pie.queue.kafka.crypto.DecryptionDeserializerConfigDef.TRY_TO_DECRYPT_MESSAGES_CONFIG;
import static com.apple.pie.queue.kafka.crypto.EncryptionSerializerConfigDef.ENCRYPT_MESSAGES_CONFIG;
import static com.apple.pie.queue.kafka.envelope.EnvelopeConfig.addExtensionSerdesToConfig;
import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;

public class ACIKafkaIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final Set<String> DO_NOT_LOG_KEYS = new HashSet<>(asList("sasl.jaas.config"));

    public static Map<TopicPartition, Triple<Long, Long, Long>> offsets(final Long startEpoc, final Long endEpoc, final Collection<String> topics, final Map<String, Object> configs) {
        return offsets(startEpoc, endEpoc, topics, configs, null);
    }

    public static Map<TopicPartition, Triple<Long, Long, Long>> offsets(final Long startEpoc, final Long endEpoc, final Collection<String> topics, final Map<String, Object> configs, final Map<String, Object> overrides) {
        Map<String, Object> config = new HashMap<>(configs);
        if (overrides != null) config.putAll(overrides);
        try (KafkaConsumer<String, ?> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(topics);
            final Map<TopicPartition, Long[]> offsets = new HashMap<>();
            // For each Topic that this consumer is currently subscribed to...
            consumer.subscription().forEach(topic -> {
                List<PartitionInfo> partitions = consumer.partitionsFor(topic);
                offsets.putAll(partitions.stream().map(info -> new TopicPartition(info.topic(), info.partition())).collect(Collectors.toMap(x -> x, x -> new Long[]{Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE})));
                if (startEpoc == null) {
                    offsets.keySet().stream().map(x -> {
                        try {
                            return Pair.of(x, consumer.position(x));
                        } catch (Exception e) {
                            return Pair.of(x, Long.MIN_VALUE);
                        }
                    }).forEach(p -> {
                        offsets.get(p.getKey())[0] = p.getRight();
                    });
                } else {
                    consumer.offsetsForTimes(offsets.keySet().stream().collect(Collectors.toMap(x -> x, x -> startEpoc))).forEach((p, o) -> {
                        if (o == null || !offsets.containsKey(p)) return;
                        offsets.get(p)[0] = o.offset();
                    });
                }
                try {
                    consumer.beginningOffsets(offsets.keySet()).forEach((p, o) -> {
                        if (o == null || !offsets.containsKey(p)) return;
                        offsets.get(p)[1] = o;
                        if (offsets.get(p)[0] == Long.MIN_VALUE) offsets.get(p)[0] = o;
                    });
                } catch (Exception e) {
                }
                if (endEpoc == null) {
                    consumer.endOffsets(offsets.keySet()).forEach((p, o) -> {
                        if (o == null || !offsets.containsKey(p)) return;
                        offsets.get(p)[2] = o;
                    });
                } else {
                    consumer.offsetsForTimes(offsets.keySet().stream().collect(Collectors.toMap(x -> x, x -> endEpoc))).forEach((p, o) -> {
                        if (o == null || !offsets.containsKey(p)) return;
                        offsets.get(p)[2] = o.offset();
                    });
                }
            });
            final Map<TopicPartition, Triple<Long, Long, Long>> map = new HashMap<>(offsets.size());
            offsets.forEach((k, v) -> map.put(k, Triple.of(v[0], v[1], v[2])));
            return map;
        }
    }

    public static Map<TopicPartition, Triple<Long, Long, Long>> offsets(final Long startEpoc, final Collection<String> topics, final Map<String, Object> configs) {
        return offsets(startEpoc, null, topics, configs, null);
    }

    public static Map<TopicPartition, Triple<Long, Long, Long>> offsets(final Instant startEpoc, final Instant endEpoc, final Collection<String> topics, final Map<String, Object> configs) {
        return offsets(startEpoc, endEpoc, topics, configs, null);
    }

    public static Map<TopicPartition, Triple<Long, Long, Long>> offsets(final Instant startEpoc, final Instant endEpoc, final Collection<String> topics, final Map<String, Object> configs, final Map<String, Object> overrides) {
        return offsets(startEpoc == null ? null : startEpoc.getMillis(), endEpoc == null ? null : endEpoc.getMillis(), topics, configs, overrides);
    }

    private static void logOffsets(final String message, final String nodeName, final String nodeType, final Map<TopicPartition, Triple<Long, Long, Long>> offsets) {
        TreeMap<TopicPartition, Triple<Long, Long, Long>> treeMap = new TreeMap<>(Comparator.comparing(x -> String.format("%s:%4d", x.topic(), x.partition())));
        treeMap.putAll(offsets);
        treeMap.forEach((k, v) -> LOGGER.info(message, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "topic", k.topic(), "partition", k.partition(), "timeOffset", v.getLeft(), "beginningOffset", v.getMiddle(), "endOffset", v.getRight(), "endVsTime", (v.getRight() - v.getLeft()), "endVsBeginning", (v.getRight() - v.getMiddle()))));
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> initRead(final Pipeline pipeline, final StargateNode node) throws Exception {
        Map<String, Object> configs = fetchKafkaConfig(pipeline, node, "deserializer");
        ACIKafkaOptions options = applyOptionsOverrides((ACIKafkaOptions) node.getConfig(), node);
        if (!options.isVanilla()) {
            configs = (new ConsumerGroupIdConfigurationInterceptor()).apply(configs);
            configs = (new ConsumerConfigurationInterceptor()).apply(configs);
            if (options.isKafkaDevMode()) {
                DevModeConfigurationInterceptor dev = new DevModeConfigurationInterceptor();
                dev.configure(configs);
                configs = dev.apply(configs);
            } else {
                KaffeConfigurationInterceptor kaffe = new KaffeConfigurationInterceptor();
                kaffe.configure(configs);
                configs = kaffe.apply(configs);
            }
        }
        return configs;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> fetchKafkaConfig(final Pipeline pipeline, final StargateNode node, final String configNodeName) throws Exception {
        ACIKafkaOptions options = applyOptionsOverrides((ACIKafkaOptions) node.getConfig(), node);
        ENVIRONMENT environment = node.environment();
        String namespaceId = options.getGroup() + DOT + options.getNamespace();
        String clientId = options.isFullyQualifiedClientId() ? options.getClientId() : (options.getGroup() + DOT + options.getClientId());
        Map<String, Object> configs = new HashMap<>();
        configs.put(CONFIG_NODE_NAME, node.name(configNodeName));
        configs.put(CONFIG_NODE_TYPE, node.getType());
        DATA_FORMAT format = options.dataFormat();
        if (format == DATA_FORMAT.avro) {
            if (options.getSchemaReference() == null) {
                appendSchemaStoreEndpointDetails(configs, environment);
            } else {
                appendSchemaStoreEndpointDetails(configs, options.getSchemaReference());
            }
        } else {
            configs.put(CONFIG_SCHEMA_ID, options.getSchemaId());
            if (!isBlank(options.getSchemaReference())) configs.put(CONFIG_SCHEMA_REFERENCE, options.getSchemaReference());
        }
        String schemaAuthorId = schemaStoreServiceName(options);
        configs.put(SCHEMA_STORE_CLIENT_SERVICE_NAME_CONFIG, schemaAuthorId);
        configs.put("pie.queue.ext.avro.schema.auto.publish", false);
        if (isBlank(options.getOffset())) {
            configs.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        } else {
            configs.put(AUTO_OFFSET_RESET_CONFIG, options.getOffset().trim());
        }
        configs.put(ENABLE_AUTO_COMMIT_CONFIG, options.isAutoCommit());
        if (!options.isVanilla()) {
            String kaffeUri = options.getUri() == null || options.getUri().isBlank() ? environment.getConfig().getAciKafkaUri() : options.getUri();
            configs.put(KAFFE_CONNECT_CONFIG, kaffeUri);
            configs.put(KAFFE_CLIENT_ID_CONFIG, clientId);
            configs.put(KAFFE_NAMESPACE_ID_CONFIG, namespaceId);
            configs.put(KAFFE_DEFAULTS_INJECTION, true);
            if (options.decryptionFlag()) {
                configs.put(TRY_TO_DECRYPT_MESSAGES_CONFIG, true);
            }
            if (options.encryptionFlag()) {
                configs.put(ENCRYPT_MESSAGES_CONFIG, true);
            }
            if (options.decryptionFlag() || options.encryptionFlag()) {
                configs.put("pie.queue.crypto.iris.client.cache.path", String.format("/tmp/crypto-%s-%s", pipelineId(), node.getName()).toLowerCase());
            }
            if (options.isKafkaDevMode()) {
                configs.put(DEV_MODE_ENABLED, true);
            }
        }
        String consumerGroupId = options.getConsumerGroupId();
        if (isBlank(consumerGroupId)) consumerGroupId = pipelineId().toLowerCase();
        configs.put(GROUP_ID_CONFIG, consumerGroupId);
        if (!isBlank(options.getPrivateKey())) {
            if (!options.isKafkaDevMode()) {
                setPrivateKeyProvider(configs, options.getPrivateKey());
            }
            configs.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        }
        if (options.getProps() != null && !options.getProps().isEmpty()) {
            configs.putAll(options.getProps());
        }
        configs.put(METRIC_REPORTER_CLASSES_CONFIG, ((options.isEnableKafkaMetrics() || AppConfig.config().getBoolean("prometheus.metrics.kafkaclient.enabled")) ? KafkaPrometheusMetricSyncer.class : KafkaNoOpMetricsReporter.class).getName());
        return configs;
    }

    private ACIKafkaOptions applyOptionsOverrides(ACIKafkaOptions options, final StargateNode node) {
        if (options == null) options = new ACIKafkaOptions();
        if ("MSK".equalsIgnoreCase(node.getType()) || "Kafka".equalsIgnoreCase(node.getType())) {
            options.setVanilla(true);
            options.setFullyQualifiedTopicName(true);
        }
        return options;
    }

    private static Map<String, Object> appendSchemaStoreEndpointDetails(final Map<String, Object> configs, final ENVIRONMENT environment) {
        SchemaStoreEndpoint schemaStoreEndpoint = appleSchemaStoreEndpoint(schemaReference(environment));
        return appendSchemaStoreEndpointDetails(configs, schemaStoreEndpoint);
    }

    private static Map<String, Object> appendSchemaStoreEndpointDetails(final Map<String, Object> configs, final String input) {
        SchemaStoreEndpoint schemaStoreEndpoint = appleSchemaStoreEndpoint(input);
        return appendSchemaStoreEndpointDetails(configs, schemaStoreEndpoint);
    }

    public String schemaStoreServiceName(final ACIKafkaOptions options) {
        return options.getClientId() + DOT + appName();
    }

    private static Map<String, Object> appendSchemaStoreEndpointDetails(final Map<String, Object> configs, final SchemaStoreEndpoint schemaStoreEndpoint) {
        configs.put(SCHEMA_STORE_PROTOCOL_CONFIG, schemaStoreEndpoint.getProtocol());
        configs.put(SCHEMA_STORE_HOST_CONFIG, schemaStoreEndpoint.getHost());
        configs.put(SCHEMA_STORE_PORT_CONFIG, schemaStoreEndpoint.getPort());
        configs.put(READ_SCHEMA_RESOLVE_FACTORY_CLASS_CONFIG, UseWriteSchema.Factory.class);
        configs.put(SCHEMA_STORE_CLASS_CONFIG, AMPSchemaStore.class);
        addExtensionSerdesToConfig(configs, SchemaHeaderSerDe.class);
        return configs;
    }

    public Map<String, Object> initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        return writeConfigs(pipeline, node);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> writeConfigs(final Pipeline pipeline, final StargateNode node) throws Exception {
        Map<String, Object> configs = fetchKafkaConfig(pipeline, node, "serializer");
        ACIKafkaOptions options = applyOptionsOverrides((ACIKafkaOptions) node.getConfig(), node);
        if (!options.isVanilla()) {
            configs = (new ProducerConfigurationInterceptor()).apply(configs);
            configs = (new ConsumerGroupIdConfigurationInterceptor()).apply(configs);
            KaffeConfigurationInterceptor kaffe = new KaffeConfigurationInterceptor();
            kaffe.configure(configs);
            configs = kaffe.apply(configs);
        }
        String producerId = options.getProducerId();
        if (!isBlank(producerId)) {
            configs.put(CLIENT_ID_CONFIG, producerId);
        }
        return configs;
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node, final Map<String, Object> configs) throws Exception {
        ACIKafkaOptions options = applyOptionsOverrides((ACIKafkaOptions) node.getConfig(), node);
        fillBootstrapServerDetails(configs, options);
        logConfigs("Kafka Reader config", node.getName(), node.getType(), configs);
        DATA_FORMAT format = options.dataFormat();
        Instant readFrom = null;
        if (options.getReadFrom() != null && !options.getReadFrom().trim().isBlank()) {
            try {
                readFrom = Instant.ofEpochMilli(parseDateTime(options.getReadFrom().trim()).toEpochMilli());
                if (readFrom == null) throw new Exception("Invalid dateTime");
            } catch (Exception e) {
                LOGGER.error("Could not parse the supplied readFrom as dateTime", Map.of("readFrom", options.getReadFrom(), NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
                throw new GenericException("Could not parse the supplied readFrom as dateTime", Map.of("readFrom", options.getReadFrom(), NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
            }
            LOGGER.debug("Kafka Reader config", Map.of("readFrom", readFrom, NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
        }
        Instant readTill = null;
        if (options.getReadTill() != null && !options.getReadTill().trim().isBlank()) {
            try {
                readTill = Instant.ofEpochMilli(parseDateTime(options.getReadTill().trim()).toEpochMilli());
                if (readTill == null) throw new Exception("Invalid dateTime");
            } catch (Exception e) {
                LOGGER.error("Could not parse the supplied readTill as dateTime", Map.of("readTill", options.getReadTill(), NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
                throw new GenericException("Could not parse the supplied readTill as dateTime", Map.of("readTill", options.getReadTill(), NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
            }
            LOGGER.debug("Kafka Reader config", Map.of("readTill", readTill, NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
        }
        List<SCollection<KV<String, GenericRecord>>> collections;
        final Map<TopicPartition, Triple<Long, Long, Long>> offsets;
        if ((options.getPollers() > 0 || !options.isUseInbuilt()) && readTill == null) {
            LOGGER.info("Using optimized unbounded Kafka reader", Map.of(NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
            configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            offsets = offsets(readFrom, null, options.getTopics() == null || options.getTopics().isEmpty() ? asList(fullQualifiedTopic(options)) : fullQualifiedTopics(options), configs);
            logOffsets("Current kafka offset details", node.getName(), node.getType(), offsets);
            CoderRegistry coderRegistry = pipeline.getCoderRegistry();
            coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<TopicPartition>() {
            }, SerializableCoder.of(TopicPartition.class)));
            AtomicInteger kvIndex = new AtomicInteger(0);
            List<KV<Integer, TopicPartition>> partitions = offsets.keySet().stream().sorted(Comparator.comparing(x -> String.format("%s:%4d", x.topic(), x.partition()))).map(o -> KV.of(kvIndex.getAndIncrement(), o)).collect(Collectors.toList());
            List<Integer> filterPartitions = options.partitions();
            if (filterPartitions != null) {
                Set<Integer> filter = new HashSet<>(filterPartitions);
                partitions = partitions.stream().filter(kv -> filter.contains(kv.getValue().partition())).collect(Collectors.toList());
            }
            List<List<KV<Integer, TopicPartition>>> partitionGroups;
            if (options.getPartitionGroups() >= 2) {
                partitionGroups = Lists.partition(partitions, partitions.size() / options.getPartitionGroups());
                LOGGER.info("Partition groups enabled ! Will create configured no of dedicated groups", Map.of(NODE_NAME, node.getName(), NODE_TYPE, node.getType(), "partitionGroups", partitionGroups.size()));
            } else {
                partitionGroups = asList(partitions);
            }
            List<PCollection<KV<String, KafkaMessage>>> readers = new ArrayList<>();
            String nodeSuffix;
            for (int i = 0; i < partitionGroups.size(); i++) {
                nodeSuffix = partitionGroups.size() == 1 ? "root" : String.format("p%d", i);
                PCollection<KV<String, KafkaMessage>> reader;
                List<KV<TopicPartition, Triple<Long, Long, Long>>> pGroup = partitionGroups.get(i).stream().map(kv -> KV.of(kv.getValue(), offsets.get(kv.getValue()))).collect(Collectors.toList());
                reader = pipeline.apply(node.name(nodeSuffix, "unbounded-reader"), org.apache.beam.sdk.io.Read.from(new KafkaSource(node.name(nodeSuffix, "unbounded-reader"), node.getType(), options, configs, pGroup, pipeline.getCoderRegistry().getCoder(KafkaMessage.class))));
                if (options.isShuffle()) reader = reader.apply(node.name(nodeSuffix, "shuffle"), Reshuffle.viaRandomKey());
                readers.add(reader);
            }
            nodeSuffix = "root";
            PCollection<KV<String, KafkaMessage>> reader = readers.size() == 1 ? readers.get(0) : PCollectionList.of(readers).apply(node.name("club-partitions"), Flatten.pCollections());
            SCollection<KV<String, GenericRecord>> collection = SCollection.of(pipeline, reader).apply(node.name(nodeSuffix), new KafkaMessageAvroConverter(node.name(nodeSuffix), node.getType(), configs, format));
            collection = collection.apply(node.name(nodeSuffix, "filter-errors"), new FilterErrors(node.name(nodeSuffix, "filter-errors"), node.getType(), (String) configs.get(CONFIG_NODE_NAME)));
            if (options.isLogToForwarder()) collection = collection.apply(node.name(nodeSuffix, "log-forwarder"), new LogbackWriter<>(node.name(nodeSuffix, "log-forwarder"), node.getType(), new SplunkOptions(), true));
            collections = asList(collection);
        } else {
            LOGGER.info("Using Beam's inbuilt Kafka connector!", Map.of(NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
            offsets = offsets(readFrom, readTill, options.getTopics() == null || options.getTopics().isEmpty() ? asList(fullQualifiedTopic(options)) : fullQualifiedTopics(options), configs, Map.of(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, GROUP_ID_CONFIG, String.format("offset-reader-%s-%s-%s", pipelineId(), FORMATTER_DATE_TIME_1.format(java.time.Instant.now()), Integer.toHexString((new Random()).nextInt()))));
            logOffsets("Potential kafka offset details", node.getName(), node.getType(), offsets);
            int totalReaders = options.getPartitionGroups() >= 2 && offsets.size() >= 2 && (options.getTopics() == null || options.getTopics().isEmpty()) ? options.getPartitionGroups() : 1;
            List<TopicPartition> partitions = offsets.keySet().stream().sorted(Comparator.comparing(x -> String.format("%s:%4d", x.topic(), x.partition()))).collect(Collectors.toList());
            List<List<TopicPartition>> partitionGroups = asList(partitions);
            if (offsets.size() >= 2 && totalReaders >= 2) {
                partitionGroups = Lists.partition(partitions, partitions.size() / totalReaders);
                totalReaders = partitionGroups.size();
                LOGGER.info("Multiple dedicated readers enabled ! Will create configured no of dedicated readers", Map.of(NODE_NAME, node.getName(), NODE_TYPE, node.getType(), "totalReaders", totalReaders));
            }
            collections = new ArrayList<>(totalReaders);
            for (int readerNo = 0; readerNo < totalReaders; readerNo++) {
                Map<String, Object> readerConfig;
                if (totalReaders == 1) {
                    readerConfig = configs;
                } else {
                    readerConfig = new HashMap<>(configs);
                    if (options.isUseReaderSpecificConsumer()) readerConfig.put(GROUP_ID_CONFIG, String.format("%s-r%d", readerConfig.get(GROUP_ID_CONFIG), readerNo));
                }
                KafkaIO.Read reader;
                if (options.isIncludeMetadata()) {
                    if (format == DATA_FORMAT.bytearraystring) options.setSchemaId(String.format("com.apple.aml.stargate.%s.internal.ByteArrayStringData", environment().name().toLowerCase()));
                    reader = KafkaIO.<byte[], byte[]>read().withKeyDeserializer(ByteArrayDeserializer.class).withValueDeserializer(ByteArrayDeserializer.class);
                } else if (options.isUseRawMessage()) {
                    reader = KafkaIO.<String, RawMessage>read().withKeyDeserializer(StringDeserializer.class).withValueDeserializerAndCoder(RawDeserializer.class, pipeline.getCoderRegistry().getCoder(RawMessage.class));
                } else {
                    switch (format) {
                        case avro:
                            reader = KafkaIO.<String, GenericRecord>read().withKeyDeserializer(StringDeserializer.class).withValueDeserializerAndCoder(AvroACIDeserializer.class, pipeline.getCoderRegistry().getCoder(GenericRecord.class));
                            break;
                        case bytearray:
                            reader = KafkaIO.<String, byte[]>read().withKeyDeserializer(StringDeserializer.class).withValueDeserializer(ByteArrayDeserializer.class);
                            break;
                        case bytearraystring:
                            options.setSchemaId(String.format("com.apple.aml.stargate.%s.internal.ByteArrayStringData", environment().name().toLowerCase()));
                            reader = KafkaIO.<String, String>read().withKeyDeserializer(StringDeserializer.class).withValueDeserializer(ByteArrayStringDeserializer.class);
                            break;
                        default:
                            reader = KafkaIO.<String, String>read().withKeyDeserializer(StringDeserializer.class).withValueDeserializer(StringDeserializer.class);
                            break;
                    }
                }
                if (readerConfig.containsKey(BOOTSTRAP_SERVERS_CONFIG)) reader = reader.withBootstrapServers((String) readerConfig.get(BOOTSTRAP_SERVERS_CONFIG));
                if (totalReaders != 1) {
                    reader = reader.withTopicPartitions(partitionGroups.get(readerNo));
                } else if (partitionGroups.get(0) != null && !partitionGroups.get(0).isEmpty()) {
                    reader = reader.withTopicPartitions(partitionGroups.get(0));
                    LOGGER.debug("Kafka Reader config", Map.of("partitions", partitionGroups.get(0), NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
                } else {
                    List<String> topics = fullQualifiedTopics(options);
                    LOGGER.debug("Kafka Reader config", Map.of("topics", topics, NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
                    reader = reader.withTopics(topics);
                }
                if (options.getMaxNumRecords() != null && options.getMaxNumRecords() >= 1) reader = reader.withMaxNumRecords(options.getMaxNumRecords());
                reader = reader.withConsumerConfigUpdates(readerConfig);
                switch (timePolicy(options)) {
                    case processingtime:
                        reader = reader.withProcessingTime();
                        break;
                    case logappendtime:
                        reader = reader.withLogAppendTime();
                        break;
                    case createtime:
                        reader = reader.withCreateTime(Duration.ZERO);
                        break;
                }
                reader = reader.withReadCommitted();
                if (options.getPartitionWatchDuration() != null && options.getPartitionWatchDuration().getSeconds() > 0) reader = reader.withDynamicRead(Duration.standardSeconds(options.getPartitionWatchDuration().getSeconds()));
                if (!options.isAutoCommit()) reader = reader.commitOffsetsInFinalize();
                if (readFrom != null) reader = reader.withStartReadTime(readFrom);
                if (readTill != null) reader = reader.withStopReadTime(readTill);
                String nodeSuffix = totalReaders == 1 ? "root" : String.format("r%d", readerNo);
                SCollection<KV<String, GenericRecord>> collection;
                boolean logToForwarderApplied = false;
                boolean sqlApplied = false;
                if (options.isIncludeMetadata()) {
                    PCollection<Row> rows = ((PCollection<Row>) pipeline.apply(node.name(nodeSuffix, "with-metadata"), reader.externalWithMetadata()));
                    if (options.isShuffle()) rows = rows.apply(node.name(nodeSuffix, "shuffle"), Reshuffle.viaRandomKey());
                    if (!isBlank(options.getSql())) {
                        rows = rows.apply(node.name(nodeSuffix, "sql"), SqlTransform.query(options.getSql()));
                        sqlApplied = true;
                    }
                    collection = SCollection.of(pipeline, rows).apply(node.name(nodeSuffix), new KafkaMetadataRowToKV(node.name(nodeSuffix), node.getType(), options, readerConfig, environment()));
                } else if (options.isUseRawMessage()) {
                    SCollection<KV<String, RawMessage>> rawCollection = ((SCollection<KV<String, RawMessage>>) SCollection.apply(pipeline, node.name(nodeSuffix, "raw"), reader.withoutMetadata()));
                    if (options.isShuffle()) rawCollection = rawCollection.apply(node.name(nodeSuffix, "shuffle"), Reshuffle.viaRandomKey());
                    collection = rawCollection.apply(node.name(nodeSuffix), new RawMessageAvroConverter(node.name(nodeSuffix), node.getType(), readerConfig, format));
                } else {
                    switch (format) {
                        case avro:
                            collection = (SCollection<KV<String, GenericRecord>>) SCollection.apply(pipeline, node.name(nodeSuffix), reader.withoutMetadata());
                            break;
                        case bytearray:
                            SCollection<KV<String, byte[]>> byteArrayCollection = (SCollection<KV<String, byte[]>>) SCollection.apply(pipeline, node.name(nodeSuffix), reader.withoutMetadata());
                            collection = (SCollection<KV<String, GenericRecord>>) byteArrayCollection.apply(node.name(nodeSuffix, "avro-converter"), new ByteArrayToGenericRecord(node.name(nodeSuffix, "avro-converter"), node.getType(), environment()));
                            break;
                        default:
                            SCollection<KV<String, String>> stringCollection = (SCollection<KV<String, String>>) SCollection.apply(pipeline, node.name(nodeSuffix), reader.withoutMetadata());
                            if (options.isLogToForwarder()) {
                                stringCollection = stringCollection.apply(node.name(nodeSuffix, "log-forwarder"), new LogbackWriter<>(node.name(nodeSuffix, "log-forwarder"), node.getType(), new SplunkOptions(), true), KV_STRING_VS_STRING_TAG);
                                logToForwarderApplied = true;
                            }
                            collection = (SCollection<KV<String, GenericRecord>>) stringCollection.apply(node.name(nodeSuffix, "avro-converter"), new ObjectToGenericRecord(node.name(nodeSuffix, "avro-converter"), node.getType(), options.getSchemaReference(), options.getSchemaId()));
                            break;
                    }
                }
                collection = collection.apply(node.name(nodeSuffix, "filter-errors"), new FilterErrors(node.name(nodeSuffix, "filter-errors"), node.getType(), (String) readerConfig.get(CONFIG_NODE_NAME)));
                if (!isBlank(options.getSql()) && !isBlank(options.getSchemaId()) && !sqlApplied) {
                    BeamSQLOptions sqlOptions = new BeamSQLOptions();
                    sqlOptions.setSchemaIds(options.getSchemaId());
                    collection = applyTransform(pipeline, node.name(nodeSuffix), node.getType(), sqlOptions, collection);
                }
                if (options.isLogToForwarder() && !logToForwarderApplied) collection = collection.apply(node.name(nodeSuffix, "log-forwarder"), new LogbackWriter<>(node.name(nodeSuffix, "log-forwarder"), node.getType(), new SplunkOptions(), true));
                collections.add(collection);
            }
        }
        SCollection<KV<String, GenericRecord>> output = collections.size() == 1 ? collections.get(0) : SCollection.flatten(node.name("club-partitions"), collections);
        output = output.apply(node.name("ensure-non-null-keys"), ensureNonNullableKey());
        output = output.apply(node.name("skip-null-records"), filterBy(kv -> kv.getValue() != null, node.name("skip-null-records"), node.getType()));
        return output;
    }

    private static void fillBootstrapServerDetails(final Map<String, Object> configs, final ACIKafkaOptions options) throws InvalidInputException {
        String bootstrapServers = options.getBootstrapServers();
        if (isBlank(bootstrapServers)) bootstrapServers = (String) configs.get(BOOTSTRAP_SERVERS_CONFIG);
        if (isBlank(bootstrapServers) && options.getProps() != null) bootstrapServers = (String) options.getProps().get(BOOTSTRAP_SERVERS_CONFIG);
        if (isBlank(bootstrapServers)) throw new InvalidInputException(String.format("%s config missing!!", BOOTSTRAP_SERVERS_CONFIG));
        configs.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        options.setBootstrapServers(bootstrapServers);
    }

    private String fullQualifiedTopic(final ACIKafkaOptions options) {
        return options.isFullyQualifiedTopicName() ? options.getTopic() : String.format("%s.%s.%s", options.getGroup(), options.getNamespace(), options.getTopic());
    }

    private List<String> fullQualifiedTopics(final ACIKafkaOptions options) {
        return options.getTopics().stream().map(topic -> options.isFullyQualifiedTopicName() ? topic : String.format("%s.%s.%s", options.getGroup(), options.getNamespace(), topic)).collect(Collectors.toList());
    }

    public KafkaConstants.BEAM_TIME_POLICY timePolicy(ACIKafkaOptions options) {
        if (options.getTimeStampPolicy() == null) {
            return KafkaConstants.BEAM_TIME_POLICY.processingtime;
        }
        return KafkaConstants.BEAM_TIME_POLICY.valueOf(options.getTimeStampPolicy().toLowerCase());
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final Map<String, Object> configs, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return transformCollection(pipeline, node, configs, collection, false);
    }

    public SCollection<KV<String, GenericRecord>> transformCollection(final Pipeline pipeline, final StargateNode node, final Map<String, Object> configs, final SCollection<KV<String, GenericRecord>> collection, final boolean emit) throws Exception {
        LOGGER.debug("Using simple kafka writer", Map.of(NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
        ACIKafkaOptions options = applyOptionsOverrides((ACIKafkaOptions) node.getConfig(), node);
        configs.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        DATA_FORMAT format = options.dataFormat();
        configs.put(VALUE_SERIALIZER_CLASS_CONFIG, format == DATA_FORMAT.avro ? ACIAvroSerializer.class.getName() : StringSerializer.class.getName());
        fillBootstrapServerDetails(configs, options);
        logConfigs("Kafka config", node.getName(), node.getType(), configs);
        return collection.apply(node.getName(), new KafkaPublisher(emit, options, node, configs));
    }

    private static void logConfigs(final String message, final String nodeName, final String nodeType, Map<String, Object> configs) {
        configs.entrySet().stream().filter(e -> !DO_NOT_LOG_KEYS.contains(e.getKey())).forEach(e -> LOGGER.debug(message, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(e.getKey()), "value", isRedactable(e.getKey()) || isRedactable(String.valueOf(e.getValue())) ? REDACTED_STRING : String.valueOf(e.getValue()))));
    }

    public Map<String, Object> initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        return writeConfigs(pipeline, node);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final Map<String, Object> configs, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return transformCollection(pipeline, node, configs, collection, true);
    }

    public static class FilterErrors extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
        private final String nodeName;
        private final String errorNodeName;
        private final String nodeType;

        public FilterErrors(final String nodeName, final String nodeType, final String errorNodeName) {
            try {
                this.nodeName = nodeName;
                this.nodeType = nodeType;
                this.errorNodeName = errorNodeName;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            final String schemaId = schema.getFullName();
            counter(nodeName, nodeType, schemaId, "process").inc();
            if (!"KafkaDeserializerError".equalsIgnoreCase(schema.getName())) {
                ctx.output(kv);
                incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
                return;
            }
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            Exception e = SerializationUtils.deserialize(Base64.getDecoder().decode(((String) getFieldValue(kv.getValue(), "exception")).getBytes(StandardCharsets.UTF_8)));
            ctx.output(ERROR_TAG, eRecord(errorNodeName, nodeType, "filter_errors", kv, e));
        }
    }
}
