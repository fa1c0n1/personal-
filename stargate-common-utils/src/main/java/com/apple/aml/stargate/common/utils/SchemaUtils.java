package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.BadRequestException;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.exceptions.GenericRuntimeException;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.amp.dsce.security.jwtframework.core.external.common.model.JwtKeyType;
import com.apple.amp.platform.daiquiri.lib.auth.common.model.SystemAccountEnvironment;
import com.apple.amp.schemastore.client.ReadOnlySchemaStoreClient;
import com.apple.amp.schemastore.client.ReadWriteSchemaStoreClient;
import com.apple.amp.schemastore.client.SchemaStoreClientBuilder;
import com.apple.amp.schemastore.client.SchemaStoreClientException;
import com.apple.amp.schemastore.model.NormalizationMode;
import com.apple.amp.schemastore.model.SchemaInsertionOption;
import com.apple.amp.schemastore.model.SchemaStoreEndpoint;
import com.apple.amp.schemastore.rest.v2.model.SchemaGetResponse;
import com.apple.amp.schemastore.rest.v2.model.SchemaInsertRequest;
import com.apple.amp.schemastore.rest.v2.model.SchemaInsertResponse;
import com.apple.amp.schemastore.rest.v2.model.SchemaRevisionResponse;
import com.apple.amp.schemastore.rest.v2.model.SchemaVersionResponse;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.RECORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.SCHEMA;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TOPIC;
import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.PRIVATE_KEY_HEADER_INCLUDES;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.STARGATE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.APPLE_SCHEMA_STORE_REFERENCE.PROD;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.AvroUtils.avroSchema;
import static com.apple.aml.stargate.common.utils.CsvUtils.parseCSVSchema;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlToMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.amp.schemastore.constants.SchemaStoreConstants.LATEST_REVISION;
import static com.apple.amp.schemastore.constants.SchemaStoreConstants.LATEST_VERSION;
import static com.apple.amp.schemastore.model.ContentType.AVRO_GENERIC_CONTENT_TYPE;
import static com.apple.amp.schemastore.model.NormalizationMode.AVRO_WITH_LOGICAL_TYPES;
import static com.apple.amp.schemastore.model.SchemaInsertionOption.ALLOW_BREAKING_FIELD_ORDER;
import static com.apple.amp.schemastore.model.SchemaInsertionOption.ALLOW_BREAKING_FULL_API_COMPATIBILITY;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public final class SchemaUtils {
    public static final Schema NOTHING = Schema.create(Schema.Type.NULL);
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final Cache<String, Schema> SCHEMA_CACHE = new Cache2kBuilder<String, Schema>() {
    }.entryCapacity(100).expireAfterWrite((config().hasPath("stargate.cache.schema.expiry") ? config().getInt("stargate.cache.schema.expiry") : 10), TimeUnit.MINUTES).build();
    @SuppressWarnings("unchecked")
    public static final Map<String, String> TYPE_ALIASES = (Map<String, String>) config().getAnyRef("stargate.schemaMappings.sqlToAvro");

    private SchemaUtils() {
    }

    public static SchemaStoreEndpoint appleSchemaStoreEndpoint(final String input) {
        return SchemaStoreEndpoint.of(appleSchemaStoreReference(input));
    }

    public static String appleSchemaStoreReference(final String input) {
        if (isBlank(input)) {
            return PROD.endpoint();
        }
        try {
            return PipelineConstants.APPLE_SCHEMA_STORE_REFERENCE.valueOf(input.toUpperCase()).endpoint();
        } catch (Exception e) {
            return input;
        }
    }

    public static ReadWriteSchemaStoreClient cachedSchemaStoreWriter(final String input) {
        String accountNameConfig = configPrefix() + ".schemastore.client.name";
        String secretKeyConfig = configPrefix() + ".schemastore.client.secretKey";
        String accountName = config().hasPath(accountNameConfig) ? config().getString(accountNameConfig) : STARGATE;
        return cachedSchemaStoreWriter(input, accountName, config().getString(secretKeyConfig));
    }

    public static ReadWriteSchemaStoreClient cachedSchemaStoreWriter(final String input, final String accountName, final String secret) {
        String endpoint = appleSchemaStoreReference(input);
        SchemaStoreEndpoint schemaStoreEndpoint = SchemaStoreEndpoint.of(endpoint);
        String secretKey = secret.contains(PRIVATE_KEY_HEADER_INCLUDES) ? secret : new String(Base64.getDecoder().decode(secret), StandardCharsets.UTF_8);
        ReadWriteSchemaStoreClient client = new SchemaStoreClientBuilder().schemaStoreEndpoint(schemaStoreEndpoint).serviceName("aml.stargate").systemAccountName(accountName).systemAccountSecretKey(secretKey).systemAccountEnvironment(SystemAccountEnvironment.PROD).jwtKeyType(JwtKeyType.RSA).buildReadWriteClient();
        return client;
    }

    public static Schema schema(final String schemaId) throws Exception {
        return schema(PROD.endpoint(), schemaId, LATEST_VERSION, LATEST_REVISION);
    }

    public static Schema schema(final String schemaReference, final String inputSchemaId, final int inputVersion, final int inputRevision) throws Exception {
        if (isBlank(inputSchemaId)) throw new BadRequestException("Null or Blank schemaId supplied!");
        String schemaIdTokens[] = inputSchemaId.split(SCHEMA_DELIMITER);
        String schemaId = schemaIdTokens[0];
        int version = schemaIdTokens.length >= 2 ? parseInt(schemaIdTokens[1]) : inputVersion;
        int revision = schemaIdTokens.length >= 3 ? parseInt(schemaIdTokens[2]) : inputRevision;
        String cacheKey = String.format("%s~%s:%d:%d", schemaReference, schemaId, version, revision);
        Schema schema = SCHEMA_CACHE.computeIfAbsent(cacheKey, key -> {
            ReadOnlySchemaStoreClient client = schemaStoreReader(schemaReference);
            SchemaGetResponse response = client.getSchema(schemaId, version <= 0 ? LATEST_VERSION : version, revision <= 0 ? LATEST_REVISION : revision);
            if (response == null) {
                throw new BadRequestException(schemaId + " is not a valid schemaId", Map.of(SCHEMA_ID, schemaId, "version", version, "revision", revision, "schemaReference", String.valueOf(schemaReference))).wrap();
            }
            String schemaString = response.getSchemaVersions().get(response.getSchemaVersions().size() - 1).getNormalizedSchema();
            if (schemaString == null) {
                throw new BadRequestException("Could not find/fetch schema", Map.of(SCHEMA_ID, schemaId, "version", version, "revision", revision, "schemaReference", String.valueOf(schemaReference))).wrap();
            }
            if (schemaString.trim().isBlank()) {
                throw new BadRequestException("Empty schema found", Map.of(SCHEMA_ID, schemaId, "version", version, "revision", revision, "schemaReference", String.valueOf(schemaReference))).wrap();
            }
            return new Schema.Parser().parse(schemaString);
        });
        return schema;
    }

    public static ReadOnlySchemaStoreClient schemaStoreReader(final String input) {
        String endpoint = appleSchemaStoreReference(input);
        SchemaStoreEndpoint schemaStoreEndpoint = SchemaStoreEndpoint.of(endpoint);
        ReadOnlySchemaStoreClient client = new SchemaStoreClientBuilder().schemaStoreEndpoint(schemaStoreEndpoint).serviceName("aml.stargate").buildReadOnlyClient();
        return client;
    }

    public static String getSchema(final String schemaId) throws Exception {
        return getSchema(PROD.endpoint(), schemaId, LATEST_VERSION, LATEST_REVISION);
    }

    public static String getSchema(final String schemaReference, final String inputSchemaId, final int inputVersion, final int inputRevision) throws Exception {
        try {
            return schema(schemaReference, inputSchemaId, inputVersion, inputRevision).toString();
        } catch (GenericRuntimeException e) {
            throw e.getException();
        } catch (Exception e) {
            throw e;
        }
    }

    public static Schema schema(final String schemaReference, final String schemaId) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(getSchema(schemaReference, schemaId));
    }

    public static String getSchema(final String schemaReference, final String schemaId) throws Exception {
        return getSchema(schemaReference, schemaId, LATEST_VERSION, LATEST_REVISION);
    }

    public static String getSchema(final String schemaReference, final String schemaId, final int version) throws Exception {
        return getSchema(schemaReference, schemaId, version, LATEST_REVISION);
    }

    public static Map<String, Object> getSchemaInfo(final String schemaReference, final String schemaId, final int version, final int revision) throws Exception {
        ReadOnlySchemaStoreClient client = schemaStoreReader(schemaReference);
        SchemaGetResponse response = client.getSchema(schemaId, version <= 0 ? LATEST_VERSION : version, revision <= 0 ? LATEST_REVISION : revision);
        if (response == null) {
            throw new BadRequestException(schemaId + " is not a valid schemaId", Map.of(SCHEMA_ID, String.valueOf(schemaId), "version", version, "revision", revision, "schemaReference", String.valueOf(schemaReference)));
        }
        SchemaVersionResponse vresponse = response.getSchemaVersions().get(response.getSchemaVersions().size() - 1);
        SchemaRevisionResponse vrevision = vresponse.getRevisions().get(vresponse.getRevisions().size() - 1);
        String schemaString = vresponse.getNormalizedSchema();
        if (schemaString == null) {
            throw new BadRequestException("Could not find/fetch schema", Map.of(SCHEMA_ID, String.valueOf(schemaId), "version", version, "revision", revision, "schemaReference", String.valueOf(schemaReference)));
        }
        if (schemaString.trim().isBlank()) {
            throw new BadRequestException("Empty schema found", Map.of(SCHEMA_ID, String.valueOf(schemaId), "version", version, "revision", revision, "schemaReference", String.valueOf(schemaReference)));
        }
        Map<String, Object> output = Map.of("schema", schemaString, "author", vrevision.getAuthor(), "version", vresponse.getVersion(), "revision", vrevision.getRevision(), "schemaReference", String.valueOf(schemaReference));
        return output;
    }

    public static String schemaReference(final PipelineConstants.ENVIRONMENT environment) {
        return (environment == null ? PipelineConstants.ENVIRONMENT.PROD : environment).getConfig().getSchemaReference();
    }

    @SuppressWarnings("unchecked")
    public static void doRegisterSchema(final Object iSchemas, final String schemaAuthor, final boolean allowBreakingFullAPICompatibility, final boolean allowBreakingFieldOrder, final SchemaRegistrationLambda lambda, final Map<String, Object> logHeaders, final Map<String, Object> debugInfo, final String additionalSchemaRegistrationReference, final Logger logger) throws BadRequestException {
        String comment = (schemaAuthor == null || schemaAuthor.isBlank()) ? null : ("Updated by " + schemaAuthor);
        logHeaders.put("schemaAuthor", comment);
        logHeaders.put("allowBreakingFullAPICompatibility", allowBreakingFullAPICompatibility);
        logHeaders.put("allowBreakingFieldOrder", allowBreakingFieldOrder);
        logger.debug("Register schemas request received", logHeaders);
        String schemaReference = environment().getConfig().getSchemaReference();
        List<Triple<ReadWriteSchemaStoreClient, Boolean, String>> clientPairs = (additionalSchemaRegistrationReference.equalsIgnoreCase(schemaReference)) ? asList(Triple.of(schemaStoreWriter(schemaReference), true, schemaReference)) : asList(Triple.of(schemaStoreWriter(schemaReference), true, schemaReference), Triple.of(schemaStoreWriter(additionalSchemaRegistrationReference), false, additionalSchemaRegistrationReference));
        List<Object> schemas = (iSchemas instanceof List) ? ((List<Object>) iSchemas) : Collections.singletonList(iSchemas);
        for (Triple<ReadWriteSchemaStoreClient, Boolean, String> clientDetails : clientPairs) {
            String reference = String.valueOf(clientDetails.getRight());
            for (Object schemaObject : schemas) {
                logger.debug("Registering schema", logHeaders, Map.of("schema", schemaObject, "schemaReference", reference));
                try {
                    String schema = requireNonNull(schemaObject instanceof String ? (String) schemaObject : jsonString(schemaObject));
                    Map<String, Object> response = registerSchema(clientDetails.getLeft(), schema, comment, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, null);
                    if (response == null) {
                        throw new Exception("Could not update/insert schema using schemaString " + schema);
                    }
                    if (lambda != null) {
                        lambda.apply(schema, comment, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, logHeaders, clientDetails.getLeft());
                    }
                    logger.debug("Schema registration successful for", logHeaders, Map.of("schemaReference", reference), response);
                    debugInfo.putIfAbsent((String) response.get("schemaName"), response.get("version"));
                } catch (SchemaStoreClientException e) {
                    if (clientDetails.getMiddle()) {
                        throw new BadRequestException(e.getMessage(), Map.of("schema", schemaObject, "schemaReference", reference), e);
                    }
                    logger.warn("Could not register schema with schema-store", Map.of("schema", schemaObject, "schemaReference", reference), e);
                } catch (Exception e) {
                    if (clientDetails.getMiddle()) {
                        throw new BadRequestException("Invalid input Schema", Map.of("schema", schemaObject), e);
                    }
                    logger.warn("Could not register schema with schema-store", Map.of("schema", schemaObject, "schemaReference", reference), e);
                }
            }
        }

        logger.debug("Schemas registered/updated successfully", logHeaders);
    }

    public static ReadWriteSchemaStoreClient schemaStoreWriter(final String input) {
        String accountNameConfig = configPrefix() + ".schemastore.client.name";
        String secretKeyConfig = configPrefix() + ".schemastore.client.secretKey";
        String accountName = config().hasPath(accountNameConfig) ? config().getString(accountNameConfig) : STARGATE;
        return schemaStoreWriter(input, accountName, config().getString(secretKeyConfig));
    }

    public static Map<String, Object> registerSchema(final ReadWriteSchemaStoreClient client, final String schema, final String iComments, final boolean allowBreakingFullAPICompatibility, final boolean allowBreakingFieldOrder, final String normalizationMode) throws BadRequestException {
        SchemaInsertResponse response = insertSchema(client, schema, iComments, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, normalizationMode);
        return Map.of("schemaName", response.getSchemaMetadata().getName(), "version", response.getVersion(), "revision", response.getRevision());
    }

    public static ReadWriteSchemaStoreClient schemaStoreWriter(final String input, final String accountName, final String secret) {
        String endpoint = appleSchemaStoreReference(input);
        SchemaStoreEndpoint schemaStoreEndpoint = SchemaStoreEndpoint.of(endpoint);
        String secretKey = secret.contains(PRIVATE_KEY_HEADER_INCLUDES) ? secret : new String(Base64.getDecoder().decode(secret), StandardCharsets.UTF_8);
        ReadWriteSchemaStoreClient client = new SchemaStoreClientBuilder().schemaStoreEndpoint(schemaStoreEndpoint).serviceName("aml.stargate").systemAccountName(accountName).systemAccountSecretKey(secretKey).systemAccountEnvironment(SystemAccountEnvironment.PROD).jwtKeyType(JwtKeyType.RSA).noCache().buildReadWriteClient();
        return client;
    }

    public static SchemaInsertResponse insertSchema(final ReadWriteSchemaStoreClient client, final String schemaString, final String iComments, final boolean allowBreakingFullAPICompatibility, final boolean allowBreakingFieldOrder, final String normalizationMode) throws BadRequestException {
        Schema schema;
        try {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(schemaString);
        } catch (Exception e) {
            throw new BadRequestException("Invalid AVRO Schema", Map.of("schemaString", schemaString));
        }
        try {
            String environmentName = environment().name();
            if (asList(schema.getNamespace().toUpperCase().split("\\.")).stream().noneMatch(x -> environmentName.equals(x))) {
                throw new BadRequestException("Could not find environment matching indicator in the namespace", Map.of("schemaString", schemaString, "namespace", String.valueOf(schema.getNamespace()), "expectedIndicator", environmentName.toUpperCase()));
            }
        } catch (BadRequestException e) {
            throw e;
        } catch (Exception e) {
            throw new BadRequestException("Invalid AVRO Namespace", Map.of("schemaString", schemaString));
        }

        Set<SchemaInsertionOption> insertionOptions = new HashSet<>();
        if (allowBreakingFullAPICompatibility) {
            insertionOptions.add(ALLOW_BREAKING_FULL_API_COMPATIBILITY);
        }
        if (allowBreakingFieldOrder) {
            insertionOptions.add(ALLOW_BREAKING_FIELD_ORDER);
        }
        String comment = iComments == null ? "N/A" : iComments;
        SchemaInsertRequest.SchemaInsertRequestBuilder builder;
        if (insertionOptions.isEmpty()) {
            builder = SchemaInsertRequest.builder().schema(schemaString).comment(comment).contentType(AVRO_GENERIC_CONTENT_TYPE);
        } else {
            builder = SchemaInsertRequest.builder().schema(schemaString).comment(comment).contentType(AVRO_GENERIC_CONTENT_TYPE).insertionOptions(insertionOptions);
        }
        if (normalizationMode == null) {
            builder = builder.normalizationMode(AVRO_WITH_LOGICAL_TYPES);
        } else {
            builder = normalizationMode.trim().isBlank() ? builder : builder.normalizationMode(NormalizationMode.valueOf(normalizationMode.toUpperCase()));
        }
        return client.insertSchema(builder.build());
    }

    public static Schema fetchSchema(final String schemaReference, final String input) {
        return fetchSchema(schemaReference, input, LATEST_VERSION, null, 0, 10);
    }

    public static Schema fetchSchema(final String schemaReference, final String input, final int version) {
        return fetchSchema(schemaReference, input, version, null, 0, 10);
    }

    public static Schema fetchSchema(final String schemaReference, final String input, final int version, final Supplier<Schema> fallback) {
        return fetchSchema(schemaReference, input, version, fallback, 0, 10);
    }

    public static Schema fetchSchema(final String schemaReference, final String input, final int version, final Supplier<Schema> fallback, final int retryCount, final int maxRetryCount) {
        Exception exception = null;
        Schema schema;
        try {
            String schemaString = getSchema(schemaReference, input, version);
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(schemaString);
        } catch (Exception e) {
            exception = e;
            schema = null;
            LOGGER.trace("Could not load schema with id {} from schema store. Will try other options.", input, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())));
        }
        if (schema != null) return schema;
        if (fallback != null) {
            try {
                schema = fallback.get();
            } catch (Exception e) {
                LOGGER.trace("Could not load schema with id {} using fallback supplier. Will try other options.", input, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())));
            }
            if (schema != null) return schema;
        }
        LOGGER.debug("Could not load schema with id {} from schema store or fallback. Will try other options.", input, Map.of(ERROR_MESSAGE, String.valueOf(exception.getMessage())));
        String environment = environment().name().toLowerCase();
        if (input.startsWith("com.apple.aml.stargate." + environment + ".internal")) {
            try {
                String filename = input.substring(input.lastIndexOf('.') + 1).toLowerCase();
                String schemaString = IOUtils.resourceToString("/" + filename + ".avsc", Charset.defaultCharset());
                if (schemaString == null || schemaString.isBlank()) {
                    throw new Exception("Could not load internal schemaString");
                }
                schemaString = schemaString.replaceAll("\\#\\{ENV\\}", environment);
                Schema.Parser parser = new Schema.Parser();
                schema = parser.parse(schemaString);
                return schema;
            } catch (Exception e) {
                if (exception != null) {
                    exception = e;
                }
                LOGGER.debug("Looks like there is no internal schema with id {}. Will try other options.", input, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())));
            }
        }
        try {
            Class pojoClass = Class.forName(input);
            if (pojoClass == null) {
                throw new Exception(input + " is not a valid className");
            }
            schema = avroSchema(pojoClass);
        } catch (Exception | Error er) {
            if (exception != null) {
                exception = new Exception(er.getMessage(), er.getCause());
            }
            LOGGER.debug("Looks like there is no class with className {} in classpath. Will try other options.", input, Map.of(ERROR_MESSAGE, String.valueOf(er.getMessage())));
            schema = null;
        }
        if (schema != null) {
            return schema;
        }
        try {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(input);
            return schema;
        } catch (Exception e) {
            if (retryCount >= maxRetryCount) {
                LOGGER.error("Could not find/load schema with id {}", input, exception == null ? e : exception);
                if (exception == null) {
                    throw new RuntimeException(e);
                } else {
                    if (exception instanceof GenericException) {
                        throw ((GenericException) exception).wrap();
                    } else {
                        throw new RuntimeException(exception);
                    }
                }
            } else {
                LOGGER.warn("Could not load schema with id {}", input, exception == null ? e : exception);
                try {
                    Thread.sleep(retryCount * 500);
                } catch (Exception ignored) {
                }
                return fetchSchema(schemaReference, input, version, fallback, retryCount + 1, maxRetryCount);
            }
        }
    }

    public static Schema simpleSchema(final Schema schema) {
        switch (schema.getType()) {
            case ENUM:
                return Schema.create(Schema.Type.STRING);
            case RECORD:
                List<Field> fields = schema.getFields().stream().map(f -> new Field(f.name(), simpleSchema(f.schema()), f.doc(), f.defaultVal(), f.order())).collect(Collectors.toList());
                return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), fields);
            case ARRAY:
                return Schema.createArray(simpleSchema(schema.getElementType()));
            case UNION:
                return Schema.createUnion(schema.getTypes().stream().map(t -> simpleSchema(t)).collect(Collectors.toList()));
            case MAP:
                return Schema.createMap(simpleSchema(schema.getValueType()));
            default:
                return schema;
        }
    }

    public static Schema rowSchema(final Schema schema, final boolean flatten, final boolean simplify) {
        List<Schema.Field> fields = new ArrayList<>();
        if (flatten) {
            schema.getFields().stream().forEach(f -> fields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())));
            fields.add(new Schema.Field(String.format("_%s", KEY), Schema.create(Schema.Type.STRING), String.format("_%s", KEY), (Object) null));
            fields.add(new Schema.Field(String.format("_%s", SCHEMA_ID), Schema.create(Schema.Type.STRING), String.format("_%s", SCHEMA_ID), (Object) null));
        } else {
            fields.add(new Schema.Field(RECORD, schema, RECORD, (Object) null));
            fields.add(new Schema.Field(KEY, Schema.create(Schema.Type.STRING), KEY, (Object) null));
            fields.add(new Schema.Field(SCHEMA, Schema.createMap(Schema.create(Schema.Type.STRING)), SCHEMA, (Object) null));
        }
        Schema rowSchema = Schema.createRecord(schema.getName(), schema.getDoc(), String.format("%s.sg.row", schema.getNamespace()).replace('-', '_'), schema.isError(), fields);
        Schema newSchema = simplify ? simpleSchema(rowSchema) : rowSchema;
        return newSchema;
    }

    public static Schema kafkaRecordSchema(final Schema schema) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field(KEY, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), KEY, (Object) null));
        fields.add(new Schema.Field("payload", Schema.createUnion(Schema.create(Schema.Type.NULL), schema), "payload", (Object) null));
        fields.add(new Schema.Field(SCHEMA, Schema.createMap(Schema.create(Schema.Type.STRING)), SCHEMA, (Object) null));
        fields.add(new Schema.Field(TOPIC, Schema.create(Schema.Type.STRING), TOPIC, (Object) null));
        fields.add(new Schema.Field("partition", Schema.create(Schema.Type.INT), "partition", (Object) null));
        fields.add(new Schema.Field("offset", Schema.create(Schema.Type.LONG), "offset", (Object) null));
        fields.add(new Schema.Field("timestamp", Schema.create(Schema.Type.LONG), "timestamp", (Object) null));
        fields.add(new Schema.Field("timestampTypeName", Schema.create(Schema.Type.STRING), "timestampTypeName", (Object) null));
        fields.add(new Schema.Field("headers", Schema.createMap(Schema.create(Schema.Type.STRING)), "headers", (Object) null));
        Schema kafkaSchema = Schema.createRecord(schema.getName(), schema.getDoc(), String.format("%s.sg.kafka.record", schema.getNamespace()).replace('-', '_'), schema.isError(), fields);
        return kafkaSchema;
    }

    public static Schema derivedSchema(final Schema base, final String namespace, final String override, final Set<String> _includes, final Set<String> excludes, final Pattern regexPattern, final boolean simplify, final boolean hierarchical) {
        try {
            Schema schema;
            Set<String> includes = _includes;
            if (isBlank(override)) {
                schema = base;
            } else {
                Schema overrideSchema = new Schema.Parser().parse(override);
                LinkedHashMap<String, Field> fieldsMap = new LinkedHashMap<>();
                base.getFields().forEach(f -> fieldsMap.put(f.name(), f));
                overrideSchema.getFields().forEach(f -> fieldsMap.put(f.name(), f));
                List<Schema.Field> fields = new ArrayList<>();
                fieldsMap.forEach((k, f) -> fields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())));
                Schema updatedSchema = Schema.createRecord(base.getName(), base.getDoc(), String.format("%s.%s", base.getNamespace(), namespace).replace('-', '_'), base.isError(), fields);
                schema = updatedSchema;
                if (includes != null) includes.addAll(overrideSchema.getFields().stream().map(f -> f.name()).collect(Collectors.toSet()));
            }
            List<Schema.Field> fields = new ArrayList<>();
            if (hierarchical) {
                final Map<String, Field> fieldMap = new HashMap<>();
                schema.getFields().forEach(field -> fillFlattenedFieldMap(field, fieldMap, EMPTY_STRING));
                Stream<String> stream = fieldMap.keySet().stream();
                if (includes != null && !includes.isEmpty()) stream = stream.filter(f -> includes.contains(f) || includes.stream().anyMatch(i -> f.startsWith(i)));
                if (excludes != null && !excludes.isEmpty()) stream = stream.filter(f -> !excludes.contains(f));
                if (regexPattern != null) stream = stream.filter(f -> regexPattern.matcher(f).find());
                Set<String> filteredFieldNames = stream.collect(Collectors.toSet());
                schema.getFields().forEach(field -> applyFilteredField(EMPTY_STRING, field, filteredFieldNames, fields));
            } else {
                Stream<Field> stream = schema.getFields().stream();
                if (includes != null) stream = stream.filter(f -> includes.contains(f.name()));
                if (excludes != null) stream = stream.filter(f -> !excludes.contains(f.name()));
                if (regexPattern != null) stream = stream.filter(f -> regexPattern.matcher(f.name()).find());
                stream.forEach(f -> fields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())));
            }
            Schema updatedSchema = Schema.createRecord(base.getName(), base.getDoc(), String.format("%s.%s", base.getNamespace(), namespace).replace('-', '_'), base.isError(), fields);
            Schema newSchema = simplify ? simpleSchema(updatedSchema) : updatedSchema;
            return newSchema;
        } catch (Exception e) {
            LOGGER.error("Could not create derived schema!!", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "inputSchemaId", base.getFullName()), e);
            throw new RuntimeException(e);
        }
    }

    private static void fillFlattenedFieldMap(final Field field, final Map<String, Field> map, final String prefix) {
        Schema schema = field.schema();
        String fieldName = String.format("%s%s", prefix, field.name());
        map.put(fieldName, field);
        if (schema.getType() == Schema.Type.UNION) schema = schema.getTypes().stream().filter(f -> f.getType() != Schema.Type.NULL).findFirst().get();
        if (schema.getType() != Schema.Type.RECORD) return;
        String fieldPrefix = String.format("%s.", fieldName);
        schema.getFields().forEach(f -> fillFlattenedFieldMap(f, map, fieldPrefix));
    }

    private static void applyFilteredField(final String prefix, final Field current, final Set<String> keys, final List<Schema.Field> fields) {
        String fieldName = String.format("%s%s", prefix, current.name());
        Schema schema = current.schema();
        boolean union = schema.isUnion();
        if (union) schema = schema.getTypes().stream().filter(f -> f.getType() != Schema.Type.NULL).findFirst().get();
        if (schema.getType() != Schema.Type.RECORD) {
            if (!keys.contains(fieldName)) return;
            Field updated = new Schema.Field(current.name(), current.schema(), current.doc(), current.defaultVal());
            fields.add(updated);
            return;
        }
        List<Schema.Field> subFields = new ArrayList<>();
        String fieldPrefix = String.format("%s.", fieldName);
        schema.getFields().forEach(field -> applyFilteredField(fieldPrefix, field, keys, subFields));
        if (subFields.isEmpty()) return;
        Schema updated = Schema.createRecord(current.name(), current.doc(), null, false, subFields);
        if (union) updated = Schema.createUnion(current.schema().getTypes().stream().filter(f -> f.getType() == Schema.Type.NULL).findFirst().get(), updated);
        fields.add(new Schema.Field(current.name(), updated, current.doc(), current.defaultVal()));
    }

    @FunctionalInterface
    public interface SchemaRegistrationLambda {
        void apply(final String schemaString, String comment, Boolean allowBreakingFullAPICompatibility, Boolean allowBreakingFieldOrder, Map<String, Object> logHeaders, ReadWriteSchemaStoreClient client) throws Exception;
    }

    public static Schema getLocalSchema(final PipelineConstants.ENVIRONMENT environment, final String fileName) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = IOUtils.resourceToString("/" + fileName, Charset.defaultCharset()).replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static Schema parseSchema(final String schemaId, final Object object) {
        Map<String, Object> map;
        try {
            String schemaString = (object instanceof String) ? ((String) object).trim() : jsonString(object);
            Schema schema = schemaString.startsWith("{") ? new Schema.Parser().parse(schemaString) : parseCSVSchema(schemaId, schemaString, false);
            if (schema == null) throw new Exception();
            return schema;
        } catch (Exception e) {
            map = (object instanceof String) ? yamlToMap((String) object) : (object instanceof Map) ? (Map<String, Object>) object : (Map<String, Object>) (Map) readJsonMap(jsonString(object));
        }
        if (map == null) throw new InvalidInputException("Invalid schema");
        String[] schemaTokens = schemaId.trim().split("\\.");
        if (schemaTokens.length <= 1) throw new InvalidInputException("Invalid schemaId. SchemaId should include both namespace & name", Map.of(SCHEMA_ID, schemaId));
        List<Object> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            fields.add(avroSchemaNode(entry.getKey(), entry.getValue()));
        }
        Map<String, Object> spec = Map.of("type", "record", "name", schemaTokens[schemaTokens.length - 1], "namespace", Arrays.stream(schemaTokens, 0, schemaTokens.length - 1).collect(Collectors.joining(".")), "fields", fields);
        String schemaString = jsonString(spec);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private static Map<String, Object> avroSchemaNode(final String key, Object object) {
        if (object instanceof String) {
            String fieldType = ((String) object).trim().toLowerCase();
            fieldType = TYPE_ALIASES.getOrDefault(fieldType, fieldType);
            return Map.of("name", key, "type", asList("null", fieldType));
        }
        if (object instanceof List) {
            String fieldType = ((List<String>) object).get(0);
            fieldType = TYPE_ALIASES.getOrDefault(fieldType, fieldType);
            return Map.of("name", key, "type", asList("null", Map.of("type", "array", "items", fieldType)));
        }
        if (object instanceof Map) {
            List<Object> fields = new ArrayList<>();
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) object).entrySet()) {
                fields.add(avroSchemaNode(entry.getKey(), entry.getValue()));
            }
            return Map.of("name", key, "type", asList("null", Map.of("type", "record", "name", String.format("%s_type", key), "fields", fields)));
        }
        throw new InvalidInputException(String.format("Could not determine/convert node associated with key - %s", key), Map.of("avroKey", key, "avroValue", String.valueOf(object)));
    }
}
