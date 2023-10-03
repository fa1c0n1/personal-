package com.apple.aml.stargate.app.service;

import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.aml.stargate.common.utils.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.A3Constants.CALLER_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.KafkaConstants.EAI_DHARI_FIELDS.DHARI_TABLE_NAME;
import static com.apple.aml.stargate.common.constants.KafkaConstants.EAI_DHARI_FIELDS.DHARI_TABLE_TYPE;
import static com.apple.aml.stargate.common.constants.KafkaConstants.EAI_DHARI_FIELDS.DHARI_TIME_STAMP;
import static com.apple.aml.stargate.common.constants.KafkaConstants.EAI_DHARI_FIELDS.DHARI_TRANSACTION_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.ENVIRONMENT;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.CsvUtils.readSimpleSchema;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.doRegisterSchema;
import static com.apple.aml.stargate.common.utils.SchemaUtils.getSchemaInfo;
import static com.apple.aml.stargate.common.utils.SchemaUtils.registerSchema;
import static java.util.Map.of;

@Service
public class SchemaProxyService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final ENVIRONMENT environment = environment();
    @Value("${schemas.avro.additional.registration.schemaReference}")
    private String additionalSchemaRegistrationReference;
    @Autowired
    private InfraService infraService;

    @SuppressWarnings("unchecked")
    public Mono<ResponseBody> registerEAISchemas(final Long appId, final Object iSchemas, final String iSchemaAuthor, final boolean allowBreakingFullAPICompatibility, final boolean allowBreakingFieldOrder, final ServerHttpRequest request) {
        return registerAppleSchemas(appId, iSchemas, iSchemaAuthor, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, request, (schemaString, comment, allowBreakingFullAPICompatibility1, allowBreakingFieldOrder1, logHeaders, client) -> {
            Parser parser = new Parser();
            Schema schema = parser.parse(schemaString);
            Map<Object, Object> schemaJson = readJsonMap(schemaString);
            schemaJson.put("name", "Supplement");
            schemaJson.put("namespace", schema.getFullName());
            List fields = new ArrayList();
            fields.add(of("name", DHARI_TIME_STAMP.name(), "type", "long", "logicalType", "timestamp-micros"));
            fields.add(of("name", DHARI_TRANSACTION_ID.name(), "type", "string"));
            fields.add(of("name", DHARI_TABLE_NAME.name(), "type", "string"));
            fields.add(of("name", DHARI_TABLE_TYPE.name(), "type", "string"));
            for (Object f : (List) schemaJson.get("fields")) {
                fields.add(readJsonMap(jsonString(f)));
            }
            schemaJson.put("fields", fields);
            String augmentedSchema = jsonString(schemaJson);
            LOGGER.debug("Registering EAI Augmented Schema", logHeaders, Map.of("schema", schema, "supplementSchema", augmentedSchema));
            registerSchema(client, augmentedSchema, comment, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, null);
        });
    }

    @SuppressWarnings("unchecked")
    private Mono<ResponseBody> registerAppleSchemas(final Long appId, final Object iSchemas, final String schemaAuthor, final boolean allowBreakingFullAPICompatibility, final boolean allowBreakingFieldOrder, final ServerHttpRequest request, final SchemaUtils.SchemaRegistrationLambda lambda) {
        return Mono.create(sink -> {
            Map<String, Object> logHeaders = infraService.logHeaders(request);
            logHeaders.put(CALLER_APP_ID, appId);
            Map<String, Object> debugInfo = new HashMap<>();
            try {
                doRegisterSchema(iSchemas, schemaAuthor, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, lambda, logHeaders, debugInfo, additionalSchemaRegistrationReference, LOGGER);
                sink.success(new ResponseBody<>("Schemas registered successfully", debugInfo));
            } catch (Exception e) {
                LOGGER.error("Failed to register schemas ", logHeaders, e);
                sink.error(e);
            }
        });
    }

    public Mono<ResponseBody> registerSchemas(final Long appId, final Object iSchemas, final String iSchemaAuthor, final boolean allowBreakingFullAPICompatibility, final boolean allowBreakingFieldOrder, final ServerHttpRequest request) {
        return registerAppleSchemas(appId, iSchemas, iSchemaAuthor, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, request, null);
    }

    public Mono<? extends Map<String, Object>> fetchLatestSchema(final Long appId, final String schemaId, final ServerHttpRequest request) {
        return fetchSchema(appId, schemaId, null, request);
    }

    public Mono<Map<String, Object>> fetchSchema(final Long appId, final String schemaId, final Integer versionNo, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            Map<String, Object> logHeaders = infraService.logHeaders(request);
            logHeaders.put(CALLER_APP_ID, appId);
            logHeaders.put(SCHEMA_ID, schemaId);
            logHeaders.put("version", versionNo + "");
            LOGGER.debug("fetchSchema received for", logHeaders);
            try {
                Map<String, Object> info = getSchemaInfo(environment.getConfig().getSchemaReference(), schemaId, versionNo == null ? 0 : versionNo.intValue(), -1);
                LOGGER.debug("Schema fetched successfully", logHeaders);
                Map json;
                try {
                    json = readJson((String) info.get("schema"), Map.class);
                } catch (Exception e) {
                    LOGGER.warn("Could not parse normalizedSchema as JSON", e);
                    json = null;
                }
                if (json == null) {
                    sink.success(info);
                } else {
                    Map<String, Object> map = new HashMap<>(info);
                    map.put("schemaJson", json);
                    sink.success(map);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to fetch schema ", logHeaders, e);
                sink.error(e);
            }
        });
    }

    public Mono<? extends ResponseBody> registerCSVSchema(final Long appId, final String schemaId, final String csv, final String schemaAuthor, final boolean allowBreakingFullAPICompatibility, final boolean allowBreakingFieldOrder, final boolean includesHeader, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            Map<String, Object> logHeaders = infraService.logHeaders(request);
            logHeaders.put(CALLER_APP_ID, appId);
            logHeaders.put(SCHEMA_ID, schemaId);
            logHeaders.put("includesHeader", includesHeader);
            Map<String, Object> debugInfo = new HashMap<>();
            LOGGER.debug("registerCSVSchema received. Will convert to AVRO Spec & register", logHeaders);
            try {
                String schemaString = readSimpleSchema(schemaId, csv, includesHeader);
                logHeaders.put("schemaString", schemaString);
                LOGGER.debug("Converted CSV Spec to AVRO Spec successfully", logHeaders);
                doRegisterSchema(schemaString, schemaAuthor, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, null, logHeaders, debugInfo, additionalSchemaRegistrationReference, LOGGER);
                debugInfo.put("schema", readJsonMap(schemaString));
                sink.success(new ResponseBody<>("Schema registered successfully", debugInfo));
            } catch (Exception e) {
                LOGGER.error("Failed to register csv schema ", logHeaders, e);
                sink.error(e);
            }
        });
    }
}
