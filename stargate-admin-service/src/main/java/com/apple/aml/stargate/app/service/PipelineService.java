package com.apple.aml.stargate.app.service;

import com.apple.aiml.spi.surface.client.rest.model.DeploymentRequest;
import com.apple.aiml.spi.surface.client.rest.model.DeploymentResponse;
import com.apple.aml.stargate.app.config.AppDefaults;
import com.apple.aml.stargate.app.pojo.GithubPathDetails;
import com.apple.aml.stargate.app.pojo.PipelineInfo;
import com.apple.aml.stargate.app.pojo.PipelineMetadata;
import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants.RUNNER;
import com.apple.aml.stargate.common.exceptions.BadRequestException;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.options.DeploymentOptions;
import com.apple.aml.stargate.common.options.HttpConnectionOptions;
import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.common.web.clients.AppConfigClient;
import com.apple.aml.stargate.datahub.ingestion.StargateDatahubIngestion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.SneakyThrows;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.apple.aml.stargate.common.constants.A3Constants.KNOWN_APP.DHARI;
import static com.apple.aml.stargate.common.constants.A3Constants.KNOWN_APP.STARGATE;
import static com.apple.aml.stargate.common.constants.CommonConstants.CONFIG_PARSE_OPTIONS_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.CONFIG_RENDER_OPTIONS_CONCISE;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RUN_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.VERSION_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.API_VERSION;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.KIND;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.ACTIVE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.CREATED_ON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.SAVED_ON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.UPDATED_ON;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEPLOYMENT_RESOURCE_NAME;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_SURFACE_BASE_PATH;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_SURFACE_QUEUE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PROPERTY_PIPELINE_APP_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PROPERTY_PIPELINE_TOKEN;
import static com.apple.aml.stargate.common.utils.A3Utils.a3Mode;
import static com.apple.aml.stargate.common.utils.A3Utils.getA3Token;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.EncryptionUtils.encodedRSAKeyPair;
import static com.apple.aml.stargate.common.utils.EncryptionUtils.getPublicKeyFromPrivateKey;
import static com.apple.aml.stargate.common.utils.EncryptionUtils.pemString;
import static com.apple.aml.stargate.common.utils.EncryptionUtils.privateKey;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconToPojo;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonToYaml;
import static com.apple.aml.stargate.common.utils.JsonUtils.nonNullValueMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlString;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlToMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SurfaceUtils.createDeploymentRequest;
import static com.apple.aml.stargate.common.utils.SurfaceUtils.getDeploymentApi;
import static com.apple.aml.stargate.common.utils.SurfaceUtils.getSurfDawToken;
import static com.apple.aml.stargate.common.utils.WebUtils.newOkHttpClient;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.push;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static com.mongodb.client.model.Updates.unset;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Base64.getDecoder;
import static java.util.Base64.getEncoder;

@Service
public class PipelineService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final String COLLECTION_DEFINITION = "pipeline";
    private static final String COLLECTION_HISTORY = "pipeline_history";
    private static final FindOneAndUpdateOptions OPTION_UPDATE = new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER);
    @Autowired
    private MongoDatabase db;
    @Autowired
    private StargateDatahubIngestion stargateDatahubIngestion;
    @Value("${application.syncToDatahub}")
    private boolean syncToDatahub;
    private OkHttpClient surfaceHttpClient;
    @Autowired
    private AppDefaults appDefaults;

    @PostConstruct
    @SneakyThrows
    private void initializeSurfaceHttpClient() {
        HttpConnectionOptions httpOptions = appDefaults.getConnectionOptions();
        surfaceHttpClient = newOkHttpClient(httpOptions, () -> Map.of("Cookie", "acack=" + getSurfDawToken()));
        setDataCatalogEnv();
    }

    @SuppressWarnings("unchecked")
    public Mono<String> getStargateK8sManifest(final long appId, final String runnerType, final String pipelineId, final Map<String, Object> overrides, final boolean localDebugging, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                Document doc = getPipelineDefinitionDoc(appId, pipelineId);
                for (String key : asList("definition", "spec", "appId", "mode")) {
                    doc.remove(key);
                }
                if (overrides != null) {
                    for (String key : asList("definition", "spec", "properties")) overrides.remove(key);
                    doc.putAll(overrides);
                }
                if (!isBlank(runnerType)) {
                    RUNNER runner = RUNNER.valueOf(runnerType.trim().toLowerCase());
                    doc.put("runner", runner.name());
                }
                if (localDebugging) {
                    doc.put("debugMode", "true");
                    doc.put("apmMode", "true");
                }
                Map definition = nonNullValueMap(parseDeploymentOptions(doc));
                definition.remove(PIPELINE_ID);
                if (!definition.containsKey("deploymentSize")) definition.put("deploymentSize", PipelineConstants.DEPLOYMENT_SIZE.s.name());
                LinkedHashMap map = new LinkedHashMap();
                map.put("apiVersion", API_VERSION);
                map.put("kind", KIND);
                map.put("metadata", Map.of("name", pipelineId));
                map.put("spec", definition);
                sink.success(yamlString(map));
            } catch (Exception e) {
                LOGGER.error("Failed to fetch k8s manifest", Map.of(PIPELINE_ID, pipelineId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    public Document getPipelineDefinitionDoc(final long appId, final String pipelineId) throws Exception {
        Document doc = db.getCollection(COLLECTION_DEFINITION).find(eq(PIPELINE_ID, pipelineId)).limit(1).first();
        long existingAppId = doc == null ? -1 : doc.getInteger("appId", -1);
        if (existingAppId <= 0) {
            DeploymentOptions legacy = fetchLegacyPipelineDefinition(pipelineId);
            if (appId != legacy.getAppId() && !(appId == STARGATE.appId() || appId == DHARI.appId())) {
                throw new SecurityException("Not authorized to create Pipeline for another appId");
            }
            doc = persistPipelineDefinition(legacy);
        }
        return doc;
    }

    @SuppressWarnings("unchecked")
    public static DeploymentOptions parseDeploymentOptions(final Map map) { // TODO : Below code is to ensure api's are backward compatible; we need to remove these usages slowly
        for (final String mergeKey : asList("flink", "spark")) {
            Map<String, Object> mergeMap = (Map<String, Object>) map.get(mergeKey);
            if (mergeMap == null || mergeMap.isEmpty()) {
                continue;
            }
            for (Map.Entry<String, Object> entry : mergeMap.entrySet()) {
                map.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        Map<String, Object> envVariables = (Map<String, Object>) map.remove("envVariables");
        if (envVariables != null && !envVariables.isEmpty()) {
            Map globals = (Map) map.computeIfAbsent("globals", k -> new HashMap<>());
            for (Map.Entry<String, Object> entry : envVariables.entrySet()) {
                globals.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        Object nodeSelector = map.get("nodeSelector");
        if (nodeSelector != null && nodeSelector instanceof String) {
            map.put("nodeSelector", Map.of("node.kubernetes.io/instance-type", String.valueOf(nodeSelector)));
        }
        Object jvmOptions = map.get("jvmOptions");
        if (jvmOptions != null && jvmOptions instanceof String) {
            map.put("jvmOptions", asList(jvmOptions));
        }
        String configKeyName = "flinkConfigs";
        Map<String, Object> configs = (Map<String, Object>) map.remove(configKeyName);
        if (configs == null) {
            configKeyName = "runnerConfigs";
            configs = (Map<String, Object>) map.remove(configKeyName);
        }
        if (configs == null) {
            configKeyName = "flinkConfigs";
            configs = new HashMap<>();
        }
        for (Map.Entry<String, String> entry : Map.of("taskManagerManagedMemorySize", "taskmanager.memory.managed.size", "taskManagerTaskOffHeapMemorySize", "taskmanager.memory.task.off-heap.size", "taskManagerNetworkMaxMemorySize", "taskmanager.memory.network.max").entrySet()) {
            String value = (String) map.remove(entry.getKey());
            if (isBlank(value)) continue;
            configs.put(entry.getValue(), value);
        }
        if (!configs.isEmpty()) map.put(configKeyName, configs);
        return getAs(map, DeploymentOptions.class);
    }

    @SuppressWarnings("unchecked")
    public static DeploymentOptions fetchLegacyPipelineDefinition(final String pipelineId) throws Exception {
        String dhariUrl = environment().getConfig().getDhariUri();
        String a3Token = getA3Token(DHARI.appId());
        Map props = WebUtils.getJsonData(dhariUrl + "/restricted/pipeline/fetch/props/" + pipelineId, appId(), a3Token, a3Mode(), Map.class);
        if (props == null || props.isEmpty()) {
            throw new InvalidInputException("Could not find pipeline definition", Map.of(PIPELINE_ID, pipelineId));
        }
        long appId = parseLong(String.valueOf(props.get(PROPERTY_PIPELINE_APP_ID)));
        List<Map> files = WebUtils.getJsonData(dhariUrl + "/restricted/pipeline/list/resources/" + pipelineId, appId(), a3Token, a3Mode(), List.class);
        if (files == null || files.isEmpty() || files.stream().noneMatch(x -> DEPLOYMENT_RESOURCE_NAME.equalsIgnoreCase((String) x.get("name")))) {
            throw new BadRequestException("Pipeline is not created using stargate admin interface. No deployment found");
        }
        ByteBuffer buffer = WebUtils.getData(dhariUrl + "/restricted/pipeline/fetch/resource/" + pipelineId + "/" + DEPLOYMENT_RESOURCE_NAME, appId(), a3Token, a3Mode(), ByteBuffer.class);
        DeploymentOptions definition = parseDeploymentOptions(new String(buffer.array(), UTF_8));
        definition.setSpec(WebUtils.getData(dhariUrl + "/restricted/pipeline/fetch/resource/" + pipelineId + "/pipeline-" + pipelineId + ".yml", appId(), a3Token, a3Mode(), String.class));
        definition.setAppId(appId);
        return definition;
    }

    @SuppressWarnings("unchecked")
    private Document persistPipelineDefinition(final DeploymentOptions definition) throws Exception {
        Instant now = Instant.now();
        definition.setPipelineId(definition.getPipelineId().trim());
        String pipelineId = definition.getPipelineId();
        Map<String, Object> map = getAs(definition, Map.class);
        for (String key : asList("overrides")) {
            map.remove(key);
        }
        List<Bson> updates = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                updates.add(unset(entry.getKey()));
                continue;
            }
            if (entry.getValue() instanceof Number) {
                Number number = (Number) entry.getValue();
                if (number.doubleValue() == 0) {
                    updates.add(unset(entry.getKey()));
                    continue;
                }
            }
            updates.add(set(entry.getKey(), entry.getValue()));
        }
        updates.add(set(PIPELINE_ID, pipelineId));
        updates.add(inc(VERSION_NO, 1L));
        updates.add(setOnInsert(RUN_NO, 0L));
        updates.add(setOnInsert(CREATED_ON, now));
        updates.add(set(UPDATED_ON, now));
        updates.add(set(SAVED_ON, now));
        updates.add(set(ACTIVE, true));
        Document doc = db.getCollection(COLLECTION_DEFINITION).findOneAndUpdate(eq(PIPELINE_ID, definition.getPipelineId()), combine(updates), OPTION_UPDATE); // TODO : Will not handle unset/remove old fields from pojo definition
        db.getCollection(COLLECTION_HISTORY).findOneAndUpdate(eq(PIPELINE_ID, definition.getPipelineId()), combine(set("currentVersion", doc.getLong(VERSION_NO)), set(UPDATED_ON, now), push("versions", doc)), OPTION_UPDATE);
        return doc;
    }

    @SuppressWarnings("unchecked")
    public static DeploymentOptions parseDeploymentOptions(final String inputString) {
        Map map = inputString.trim().startsWith("{") ? readJsonMap(inputString) : yamlToMap(inputString);
        Map modifiedMap = map.containsKey("kind") ? (Map) map.get("spec") : map;
        modifiedMap.put(PIPELINE_ID, String.valueOf(map.get(PIPELINE_ID)));
        return parseDeploymentOptions(modifiedMap);
    }

    public Mono<Map<String, Object>> getLatestRunInfo(final Long appId, final String pipelineId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                String[] projections = new String[]{RUN_NO, VERSION_NO};
                Document doc = db.getCollection(COLLECTION_DEFINITION).findOneAndUpdate(eq(PIPELINE_ID, pipelineId), combine(inc(RUN_NO, 1L), set(UPDATED_ON, now())), OPTION_UPDATE.projection(include(projections)));
                Map<String, Object> map = new HashMap<>();
                for (String fieldName : projections) map.put(fieldName, doc.get(fieldName));
                sink.success(map);
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<String> getPipelineInfo(final String pipelineId) {
        return Mono.create(sink -> {
            try {
                sink.success(getPipelineDetail(pipelineId));
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    private String getPipelineDetail(final String pipelineId) throws Exception {
        PipelineInfo fetchPipelineInfo = fetchPipelineInfo(pipelineId);
        if (fetchPipelineInfo == null) {
            throw new Exception("Given Pipeline " + pipelineId + " not found");
        }
        ObjectMapper Obj = new ObjectMapper().registerModule(new JavaTimeModule()).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return Obj.writeValueAsString(fetchPipelineInfo);
    }

    public PipelineInfo fetchPipelineInfo(final String pipelineId) throws Exception {
        try {
            CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

            PipelineInfo pipelineInfo = db.withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_DEFINITION).find(eq(PIPELINE_ID, pipelineId), PipelineInfo.class).first();
            return pipelineInfo;
        } catch (Exception e) {
            LOGGER.warn("Could not fetch pipeline", Map.of(PIPELINE_ID, pipelineId, ERROR_MESSAGE, String.valueOf(e.getMessage())));
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    public Mono<Map<String, Object>> setPipelineMetadata(final String pipelineId, final PipelineMetadata pipelineMetadata, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {

                PipelineInfo pipelineInfo = new PipelineInfo();
                pipelineInfo.setPipelineId(pipelineId);
                pipelineInfo.setPipelineMetadata(pipelineMetadata);

                boolean result = false;
                PipelineInfo fetchPipelineInfo = fetchPipelineInfo(pipelineId);

                if (fetchPipelineInfo == null) {
                    sink.error(new Exception("Given Pipeline " + pipelineId + " not found"));
                    return;
                } else {
                    fetchPipelineInfo.updatePipelineInfo(pipelineInfo);
                    result = updatePipelineMetadata(pipelineId, fetchPipelineInfo);
                }

                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("dbUpdate", result);
                if (result) {
                    if (syncToDatahub) {
                        String pipelineSpec = getPipelineSpec(appId(), pipelineId);
                        pipelineSpec = pipelineSpec.replace("#{ENV}", AppConfig.mode());
                        pipelineSpec = convertSpecIntoYaml(pipelineSpec);
                        String pipelineMetadataJson = getPipelineDetail(pipelineId);
                        boolean ingestionStatus = stargateDatahubIngestion.ingestData(pipelineId
                                , pipelineSpec, pipelineMetadataJson
                                , getDataCatalogDomain(fetchPipelineInfo)
                                , A3Constants.KNOWN_APP.STARGATE.appId()
                                , A3Utils.getA3Token(A3Constants.KNOWN_APP.DATA_CATALOG.appId())
                                , AppConfig.mode());
                        resultMap.put("dataCatalogUpdate", ingestionStatus);
                    }
                    sink.success(resultMap);
                } else {
                    sink.error(new Exception("Failed to persist"));
                }
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public boolean updatePipelineMetadata(String pipelineId, PipelineInfo pipelineInfo) {
        try {
            CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

            Document document = db.getCollection(COLLECTION_DEFINITION).withCodecRegistry(pojoCodecRegistry).findOneAndUpdate(eq(PIPELINE_ID, pipelineId), combine(set("pipelineMetadata", pipelineInfo.getPipelineMetadata()),
//                                    set("nodes", pipelineInfo.getNodes()),
                    set(UPDATED_ON, now())));

            if (document == null) {
                return false;
            }

            return true;
        } catch (Exception e) {
            LOGGER.warn("Could not able to update the existing pipeline", Map.of(PIPELINE_ID, pipelineId, ERROR_MESSAGE, String.valueOf(e.getMessage())));
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private String getPipelineSpec(final long appId, final String pipelineId) throws Exception {
        Document doc = getPipelineDefinitionDoc(appId, pipelineId);
        Map<String, String> definition = (Map<String, String>) doc.get("definition");
        return definition == null || definition.isEmpty() ? doc.getString("spec") : yamlString(definition);
    }

    private String convertSpecIntoYaml(String spec) {
        try {
            JsonUtils.readJson(spec, Object.class);
            return jsonToYaml(spec);
        } catch (Exception e) {
            return spec;
        }
    }

    private void setDataCatalogEnv() {
        System.setProperty("datahub_ingestion_endpoint", environment().getConfig().getDataCatalogUri());
        System.setProperty("datahub_ingestion_enable", "true");
    }

    private String getDataCatalogDomain(PipelineInfo fetchPipelineInfo) {
        if (fetchPipelineInfo.getDomain() == null) {
            return "aml.stargate";
        } else {
            return fetchPipelineInfo.getDomain();
        }
    }

    public Mono<? extends String> getPipelineToken(final long appId, final String pipelineId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                sink.success(fetchPipelineToken(appId, pipelineId));
            } catch (Exception e) {
                sink.error(e instanceof SecurityException ? e : new SecurityException(e));
            }
        });
    }

    public static String fetchPipelineToken(final long inputAppId, final String pipelineId) throws Exception {
        String dhariUrl = environment().getConfig().getDhariUri();
        String a3Token = getA3Token(DHARI.appId());
        Map props = WebUtils.getJsonData(dhariUrl + "/restricted/pipeline/fetch/props/" + pipelineId, appId(), a3Token, a3Mode(), Map.class);
        Object propAppId = props.get(PROPERTY_PIPELINE_APP_ID);
        if (propAppId == null || (propAppId instanceof Long && (Long) propAppId != inputAppId) || (parseLong(String.valueOf(propAppId)) != inputAppId)) {
            throw new SecurityException("Not authorized to access supplied Pipeline");
        }
        return pipelineToken((String) props.get(PROPERTY_PIPELINE_TOKEN));
    }

    public static String pipelineToken(final String ccToken) throws Exception {
        return getEncoder().encodeToString(pemString(getPublicKeyFromPrivateKey(privateKey(new String(getDecoder().decode(ccToken), UTF_8))), "PUBLIC KEY").getBytes(UTF_8));
    }

    public Mono<? extends ResponseBody> createDslDataPipeline(final long appId, final String pipelineId, final String dsl, final ServerHttpRequest request) {
        String separator = StringUtils.repeat("=", 50);
        return createDataPipeline(appId, modifiedPayload(pipelineId, dsl, separator), separator, request);
    }

    public Mono<ResponseBody> createDataPipeline(final long invokerAppId, final String payload, final String separator, final ServerHttpRequest request) {
        return Mono.create(sink -> createDataPipeline(invokerAppId, payload, separator, sink));
    }

    private String modifiedPayload(final String pipelineId, final String dsl, final String separator) {
        String modifiedPayload = String.format("{\"pipelineId\": \"%s\"}\n%s\n%s", pipelineId, separator, dsl);
        return modifiedPayload;
    }

    private void createDataPipeline(final long invokerAppId, final String payload, final String separator, final MonoSink<ResponseBody> sink) {
        try {
            DeploymentOptions definition = deploymentOptions(payload, separator);
            if (definition.getAppId() <= 0) {
                definition.setAppId(invokerAppId);
            } else if (invokerAppId != definition.getAppId()) {
                if (!(invokerAppId == STARGATE.appId() || invokerAppId == DHARI.appId())) {
                    sink.error(new SecurityException("Not authorized to create Pipeline for another appId"));
                    return;
                }
            }
            String pipelineId = definition.getPipelineId();
            if (pipelineId == null || pipelineId.length() >= 51 || Pattern.compile("([^0-9A-Za-z-]+)").matcher(pipelineId).matches()) {
                throw new IllegalArgumentException("Missing/Invalid pipelineId");
            }
            Document doc = persistPipelineDefinition(definition);
            sink.success(new ResponseBody<Object>("Successfully registered pipeline. Please fetch the k8s yaml using get api and apply", Map.of(PIPELINE_ID, pipelineId, "version", doc.getLong(VERSION_NO))));
        } catch (Exception e) {
            sink.error(e);
        }
    }

    private DeploymentOptions deploymentOptions(final String payload, final String separator) throws Exception {
        DeploymentOptions definition;
        if (separator == null || separator.isBlank()) {
            definition = parseDeploymentOptions(payload);
        } else {
            int index;
            String spec;
            if ("true".equalsIgnoreCase(separator)) {
                index = payload.indexOf("\n");
                spec = payload.substring(index);
            } else {
                index = payload.indexOf(separator);
                spec = payload.substring(index + separator.length());
            }
            definition = parseDeploymentOptions(payload.substring(0, index));
            definition.setSpec(spec);
        }
        return definition;
    }

    public Mono<? extends ResponseBody> createGithubDataPipeline(final long appId, final String pipelineId, final GithubPathDetails details, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                Config config = config();
                String githubUri = environment().getConfig().getAciGitHubUri();
                String path = details.fullPath();
                String apiUrl = path.indexOf(githubUri) >= 0 ? path : String.format("https://raw.%/%", environment().getConfig().getAciGitHubUri(), path);
                String dsl = WebUtils.getData(apiUrl, null, String.class, false);
                if (dsl == null) {
                    for (final String accessToken : asList(details.getAccessToken(), config.hasPath(configPrefix() + ".github.stargateToken") ? config.getString(configPrefix() + ".github.stargateToken") : null, config.hasPath(configPrefix() + ".github.accessToken") ? config.getString(configPrefix() + ".github.accessToken") : null)) {
                        if (isBlank(accessToken)) {
                            continue;
                        }
                        try {
                            dsl = WebUtils.getData(apiUrl, Map.of("Authorization", "token " + accessToken), String.class, true);
                        } catch (Exception e) {
                            dsl = null;
                        }
                    }
                    if (dsl == null) {
                        sink.error(new SecurityException("Could not read pipeline definition using provided github details"));
                        return;
                    }
                }
                String separator = StringUtils.repeat("=", 50);
                createDataPipeline(appId, modifiedPayload(pipelineId, dsl, separator), separator, sink);
            } catch (Exception e) {
                sink.error(e instanceof SecurityException ? e : new SecurityException(e));
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Mono<? extends Map<String, String>> getPipelineProperties(final Long appId, final String pipelineId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                Document doc = getPipelineDefinitionDoc(appId, pipelineId);
                Map<String, String> properties = (Map<String, String>) doc.get("properties");
                sink.success(properties == null ? Map.of() : properties);
            } catch (Exception e) {
                LOGGER.error("Failed to fetch pipeline properties", Map.of(PIPELINE_ID, pipelineId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Mono<String> getPipelineSpec(final Long appId, final String pipelineId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                sink.success(getPipelineSpec(appId, pipelineId));
            } catch (Exception e) {
                LOGGER.error("Failed to fetch pipeline spec", Map.of(PIPELINE_ID, pipelineId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Mono<? extends Map<String, String>> getConnectInfo(final Long appId, final String pipelineId, final String connectId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                // TODO : enforce appId is authorized to get connectInfo
                String dhariUrl = environment().getConfig().getDhariUri();
                String a3Token = getA3Token(DHARI.appId());
                Map<String, String> connectInfo = WebUtils.getJsonData(dhariUrl + "/restricted/connect-info/" + connectId, appId(), a3Token, a3Mode(), Map.class);
                sink.success(connectInfo);
            } catch (Exception e) {
                LOGGER.error("Failed to fetch connect info associate with pipeline", Map.of(PIPELINE_ID, pipelineId, "connectId", connectId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Mono<? extends Map<String, String>> getConnectInfoFromAppConfig(final Long appId, final String pipelineId, final String connectId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                // TODO : enforce appId is authorized to get connectInfo

                // TODO : modify endpoint to pass module id / environment if needed | default for now
                Map<String, String> connectInfo = AppConfigClient.getProperties(connectId, null, null);
                sink.success(connectInfo);
            } catch (Exception e) {
                LOGGER.error("Failed to fetch connect info associate with pipeline", Map.of(PIPELINE_ID, pipelineId, "connectId", connectId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    public Mono<? extends ResponseBody> createDeployment(Long appId, String pipelineId, String payload, ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                LOGGER.info("Invoking deployment with details", Map.of("appId", appId, PIPELINE_ID, pipelineId));

                //Save pipeline to Mongo
                parseAndPersistPipeline(appId, modifiedPayloadYaml(pipelineId, payload, EMPTY_STRING), EMPTY_STRING);

                //Get Pipeline Spec into CoreOptions pojo
                CoreOptions options = initCoreOptions(appId, pipelineId);

                //Generate Deployment Request
                DeploymentRequest spec = createDeploymentRequest(options);

                //Call API to Deploy pipeline
                String surfaceBasePath = isBlank(request.getHeaders().getFirst(HEADER_SURFACE_BASE_PATH)) ? options.getSurfaceBasePath() : request.getHeaders().getFirst(HEADER_SURFACE_BASE_PATH);
                String surfaceQueue = isBlank(request.getHeaders().getFirst(HEADER_SURFACE_QUEUE)) ? options.getSurfaceQueue() : request.getHeaders().getFirst(HEADER_SURFACE_QUEUE);
                if (isBlank(surfaceBasePath) || isBlank(surfaceQueue)) {
                    sink.error(new BadRequestException("Surface queue or API basepath not specified!"));
                    return;
                }
                //Update the latest surface queue and path in mongo so that status, delete and other endpoints can get it from there. Not adding this logic anywhere other than create.
                db.getCollection(COLLECTION_DEFINITION).findOneAndUpdate(eq(PIPELINE_ID, pipelineId), combine(set("surfaceBasePath", surfaceBasePath), set("surfaceQueue", surfaceQueue)));

                //Call Surface API to deploy pipeline
                DeploymentResponse deploymentResponse = getDeploymentApi(surfaceHttpClient, surfaceBasePath).putDeploymentV2(surfaceQueue, pipelineId, spec, false);
                LOGGER.info("Deployment status for pipeline", Map.of("appId", appId, PIPELINE_ID, pipelineId, "response", deploymentResponse.toJson()));

                sink.success(new ResponseBody<Object>("Success", Map.of("appId", appId, PIPELINE_ID, pipelineId, "response", getAs(deploymentResponse, Map.class))));
            } catch (Exception e) {
                LOGGER.error("Failed deployment for pipeline", Map.of("appId", appId, PIPELINE_ID, pipelineId), e);
                sink.error(e);
            }
        });
    }

    private void parseAndPersistPipeline(final long invokerAppId, final String payload, final String separator) throws Exception {
        DeploymentOptions definition = deploymentOptions(payload, separator);
        if (definition.getAppId() <= 0) {
            definition.setAppId(invokerAppId);
        } else if (invokerAppId != definition.getAppId()) {
            if (!(invokerAppId == STARGATE.appId() || invokerAppId == DHARI.appId())) {
                throw new SecurityException("Not authorized to create Pipeline for another appId");
            }
        }
        String pipelineId = definition.getPipelineId();
        if (pipelineId == null || pipelineId.length() >= 51 || Pattern.compile("([^0-9A-Za-z-]+)").matcher(pipelineId).matches()) {
            throw new IllegalArgumentException("Missing/Invalid pipelineId");
        }
        Document doc = persistPipelineDefinition(definition);
        LOGGER.info("Successfully registered pipeline", Map.of(PIPELINE_ID, pipelineId, "version", doc.getLong(VERSION_NO)));
    }

    private String modifiedPayloadYaml(final String pipelineId, final String dsl, final String separator) {
        String modifiedPayload = String.format("pipelineId: %s\n%s\n%s", pipelineId, separator, dsl);
        return modifiedPayload;
    }

    private CoreOptions initCoreOptions(Long appId, String pipelineId) throws Exception {
        //Get Pipeline Spec into CoreOptions pojo
        Document doc = db.getCollection(COLLECTION_DEFINITION).find(eq(PIPELINE_ID, pipelineId)).first();
        if (doc == null || !doc.getBoolean(ACTIVE)) throw new BadRequestException("Inactive pipeline!");
        CoreOptions options = readJson(jsonString(doc), CoreOptions.class);
        options = getCoreOptions(options);
        if (options.getAppId() <= 0) options.setAppId(appId);
        options.setPipelineId(pipelineId);
        return options;
    }

    private CoreOptions getCoreOptions(CoreOptions iOptions) throws JsonProcessingException {
        if (isBlank(iOptions.getRunner())) iOptions.setRunner(PipelineConstants.RUNNER.flink.name());
        StringBuilder builder = new StringBuilder();
        builder.append(AppConfig.config().getConfig(String.format("stargate.deployment.size.%s", iOptions.deploymentSize().name())).root().render(CONFIG_RENDER_OPTIONS_CONCISE)).append("\n\n");
        builder.append(ConfigFactory.parseString(jsonString(nonNullValueMap(iOptions)), CONFIG_PARSE_OPTIONS_JSON).root().render(CONFIG_RENDER_OPTIONS_CONCISE)).append("\n\n");
        if (!isBlank(iOptions.getMerge())) builder.append(iOptions.getMerge()).append("\n\n");
        CoreOptions options = readJson(jsonString(hoconToPojo(builder.toString(), DeploymentOptions.class)), CoreOptions.class);
        if (options.getRunnerConfigs() == null) options.setRunnerConfigs(new HashMap<>());
        return options;
    }

    public Mono<? extends ResponseBody> deleteDeployment(Long appId, String pipelineId, ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                LOGGER.info("Invoking delete deployment with details", Map.of("appId", appId, PIPELINE_ID, pipelineId));

                //Get Pipeline Spec into CoreOptions pojo
                CoreOptions options = initCoreOptions(appId, pipelineId);

                //Call API to delete pipeline
                String surfaceBasePath = isBlank(request.getHeaders().getFirst(HEADER_SURFACE_BASE_PATH)) ? options.getSurfaceBasePath() : request.getHeaders().getFirst(HEADER_SURFACE_BASE_PATH);
                String surfaceQueue = isBlank(request.getHeaders().getFirst(HEADER_SURFACE_QUEUE)) ? options.getSurfaceQueue() : request.getHeaders().getFirst(HEADER_SURFACE_QUEUE);
                if (isBlank(surfaceBasePath) || isBlank(surfaceQueue)) {
                    sink.error(new BadRequestException("Surface queue or API basepath not specified!"));
                    return;
                }
                getDeploymentApi(surfaceHttpClient, surfaceBasePath).deleteDeploymentV2(surfaceQueue, pipelineId);
                LOGGER.info("Deployment deleted for pipeline", Map.of("appId", appId, PIPELINE_ID, pipelineId));

                //Set pipeline status in Stargate DB to false.
                db.getCollection(COLLECTION_DEFINITION).findOneAndUpdate(eq(PIPELINE_ID, pipelineId), set(ACTIVE, false));
                LOGGER.info("Updated deployment metadata for pipeline", Map.of("appId", appId, PIPELINE_ID, pipelineId));

                sink.success(new ResponseBody<Object>("Successfully deleted deployment", Map.of("appId", appId, PIPELINE_ID, pipelineId)));
            } catch (Exception e) {
                LOGGER.error("Failed to delete deployment for pipeline", Map.of("appId", appId, PIPELINE_ID, pipelineId), e);
                sink.error(e);
            }
        });
    }

    public Mono<? extends ResponseBody> getDeploymentStatus(Long appId, String pipelineId, ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                //Get Pipeline Spec into CoreOptions pojo
                CoreOptions options = initCoreOptions(appId, pipelineId);

                //Call API to delete pipeline
                String surfaceBasePath = isBlank(request.getHeaders().getFirst(HEADER_SURFACE_BASE_PATH)) ? options.getSurfaceBasePath() : request.getHeaders().getFirst(HEADER_SURFACE_BASE_PATH);
                String surfaceQueue = isBlank(request.getHeaders().getFirst(HEADER_SURFACE_QUEUE)) ? options.getSurfaceQueue() : request.getHeaders().getFirst(HEADER_SURFACE_QUEUE);
                if (isBlank(surfaceBasePath) || isBlank(surfaceQueue)) {
                    sink.error(new BadRequestException("Surface queue or API basepath not specified!"));
                    return;
                }
                //Call Surface API to getDeployment
                DeploymentResponse response = getDeploymentApi(surfaceHttpClient, surfaceBasePath).getDeploymentV2(surfaceQueue, pipelineId);
                sink.success(new ResponseBody<Object>("Success", Map.of("appId", appId, PIPELINE_ID, pipelineId, "response", getAs(response.getState(), Map.class))));
            } catch (Exception e) {
                LOGGER.error("Failed to get status of deployment for pipeline", Map.of("appId", appId, PIPELINE_ID, pipelineId), e);
                sink.error(e);
            }

        });
    }

    public Mono<? extends String> getOrCreatePipelineToken(final Long appId, final String pipelineId) {
        return Mono.create(sink -> {
            try {

                //Check if pipeline exists it wass created by the same incoming app id
                Document doc = getPipelineDefinitionDoc(appId, pipelineId);
                if (doc != null && doc.containsKey("pipelineToken")) {
                    sink.success(doc.getString("pipelineToken"));
                    return;
                }

                Pair<String, String> pair = encodedRSAKeyPair();
                String publicKey = pair.getLeft();
                String privateKey = pair.getRight();
                List<Bson> updates = new ArrayList<>();
                updates.add(set("pipelineToken", publicKey));
                updates.add(set("token", privateKey));
                db.getCollection(COLLECTION_DEFINITION).findOneAndUpdate(eq(PIPELINE_ID, pipelineId), updates);
                sink.success(publicKey);

            } catch (Exception e) {
                LOGGER.error("Failed to generate and save pipelineToken for pipeline", Map.of("appId", appId, PIPELINE_ID, pipelineId), e);
                sink.error(e);
            }

        });
    }
}
