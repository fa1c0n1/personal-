package com.apple.aml.stargate.app.service;

import com.apple.aml.stargate.app.config.AppDefaults;
import com.apple.aml.stargate.app.pojo.ACICleanupPipeline;
import com.apple.aml.stargate.app.pojo.ACIPipeline;
import com.apple.aml.stargate.app.pojo.KafkaTopicCleanup;
import com.apple.aml.stargate.app.pojo.ModelCleanupPipeline;
import com.apple.aml.stargate.app.pojo.ModelPipeline;
import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.AcceptedStateException;
import com.apple.aml.stargate.common.exceptions.UnauthorizedException;
import com.apple.aml.stargate.common.options.ACIKafkaOptions;
import com.apple.aml.stargate.common.options.ACIKafkaTopicOptions;
import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.EncryptionUtils;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.common.web.clients.ACIKafkaRestClient;
import org.apache.commons.lang3.time.DateUtils;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.constants.A3Constants.CALLER_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.IDMS_APP_ID;
import static com.apple.aml.stargate.common.constants.KafkaConstants.DEFAULT_APP_TYPE;
import static com.apple.aml.stargate.common.constants.KafkaConstants.TOPIC;
import static com.apple.aml.stargate.common.constants.PipelineConstants.MODEL_PIPELINE_CLIENT_ID_SUFFIX;
import static com.apple.aml.stargate.common.utils.A3Utils.a3Mode;
import static com.apple.aml.stargate.common.utils.JsonUtils.pojoToProperties;
import static com.apple.aml.stargate.common.utils.JsonUtils.propertiesToPojo;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Pairs.pairsOf;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Long.parseLong;

@Service
public class IngestionService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final PipelineConstants.ENVIRONMENT environment = AppConfig.environment();
    private final ConcurrentLinkedQueue<ACIPipeline> pipelineQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ModelPipeline> modelPipelineQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ModelCleanupPipeline> modelCleanupPipelineQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ACICleanupPipeline> aciCleanupPipelineQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<KafkaTopicCleanup> kafkaTopicCleanupQueue = new ConcurrentLinkedQueue<>();
    private Cache<String, ACIKafkaOptions> aciKafkaOptionsCache;

    @Autowired
    private AppDefaults appDefaults;
    private ScheduledExecutorService pipelineCreationPollerService;

    @PostConstruct
    void init() {
        aciKafkaOptionsCache = new Cache2kBuilder<String, ACIKafkaOptions>() {
        }.eternal(true).entryCapacity(100).permitNullValues(false).build();

        pipelineCreationPollerService = Executors.newScheduledThreadPool(5); // TODO : Configure no of threads
        pipelineCreationPollerService.scheduleAtFixedRate(() -> {
            if (modelPipelineQueue.isEmpty()) {
                return;
            }
            Date graceDate = DateUtils.addMinutes(new Date(), -30); // TODO : Configure
            ModelPipeline pipeline = modelPipelineQueue.poll();
            List<ModelPipeline> pipelines = new ArrayList<>();
            while (pipeline != null) {
                if (pipeline.latestActivityId() == null || pipeline.initDate() == null || pipeline.initDate().before(graceDate)) {
                    pipeline = modelPipelineQueue.poll();
                    continue;
                }
                String activityId = pipeline.latestActivityId();
                Map details;
                try {
                    details = ACIKafkaRestClient.activityDetails(pipeline.getApiUri(), pipeline.getApiToken(), activityId);
                    if (details == null || details.isEmpty()) {
                        throw new Exception("Invalid status details");
                    }
                    String status = (String) ((Map) details.get("entity")).get("status");
                    if ("SUCCEEDED".equalsIgnoreCase(status) || "FAILED".equalsIgnoreCase(status)) { // TODO : define enum
                        continueModelPipelineCreation(pipeline);
                        pipeline = modelPipelineQueue.poll();
                        continue;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Could not fetch activityDetails for activityId : {}", activityId);
                }
                pipelines.add(pipeline);
                pipeline = modelPipelineQueue.poll();
            }
            for (ModelPipeline pipe : pipelines) {
                modelPipelineQueue.add(pipe);
            }
        }, 1, 1, TimeUnit.MINUTES);
        pipelineCreationPollerService.scheduleAtFixedRate(() -> {
            if (modelCleanupPipelineQueue.isEmpty()) {
                return;
            }
            ModelCleanupPipeline pipeline = modelCleanupPipelineQueue.poll();
            List<ModelCleanupPipeline> pipelines = new ArrayList<>();
            while (pipeline != null) {
                if (pipeline.latestActivityId() == null) {
                    pipeline = modelCleanupPipelineQueue.poll();
                    continue;
                }
                String activityId = pipeline.latestActivityId();
                Map details;
                try {
                    details = ACIKafkaRestClient.activityDetails(pipeline.getApiUri(), pipeline.getApiToken(), activityId);
                    if (details == null || details.isEmpty()) {
                        throw new Exception("Invalid status details");
                    }
                    String status = (String) ((Map) details.get("entity")).get("status");
                    if ("SUCCEEDED".equalsIgnoreCase(status) || "FAILED".equalsIgnoreCase(status)) { // TODO : define enum
                        continuePipelineCleanup(pipeline);
                        pipeline = modelCleanupPipelineQueue.poll();
                        continue;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Could not fetch activityDetails for activityId : {}", activityId);
                }
                pipelines.add(pipeline);
                pipeline = modelCleanupPipelineQueue.poll();
            }
            for (ModelCleanupPipeline pipe : pipelines) {
                modelCleanupPipelineQueue.add(pipe);
            }
        }, 30, 30, TimeUnit.SECONDS);
        pipelineCreationPollerService.scheduleAtFixedRate(() -> {
            if (kafkaTopicCleanupQueue.isEmpty()) {
                return;
            }
            KafkaTopicCleanup topicCleanup = kafkaTopicCleanupQueue.poll();
            List<KafkaTopicCleanup> cleanups = new ArrayList<>();
            while (topicCleanup != null) {
                if (topicCleanup.getLatestActivityId() == null) {
                    topicCleanup = kafkaTopicCleanupQueue.poll();
                    continue;
                }
                String activityId = topicCleanup.getLatestActivityId();
                Map details;
                try {
                    details = ACIKafkaRestClient.activityDetails(topicCleanup.getApiUri(), topicCleanup.getApiToken(), activityId);
                    if (details == null || details.isEmpty()) {
                        throw new Exception("Invalid status details");
                    }
                    String status = (String) ((Map) details.get("entity")).get("status");
                    if ("SUCCEEDED".equalsIgnoreCase(status) || "FAILED".equalsIgnoreCase(status)) { // TODO : define enum
                        continueTopicCleanup(topicCleanup);
                        topicCleanup = kafkaTopicCleanupQueue.poll();
                        continue;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Could not fetch activityDetails for activityId : {}", activityId);
                }
                cleanups.add(topicCleanup);
                topicCleanup = kafkaTopicCleanupQueue.poll();
            }
            for (KafkaTopicCleanup cleanup : cleanups) {
                kafkaTopicCleanupQueue.add(cleanup);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    @PreDestroy
    void preClose() {
        if (pipelineCreationPollerService != null) {
            pipelineCreationPollerService.shutdown();
        }
    }

    private ACIKafkaOptions getACIKafkaOptions(final String type, final String appId, final String topic) {
        return getACIKafkaOptions(type, appId, topic, true);
    }

    @SuppressWarnings("unchecked")
    private ACIKafkaOptions getACIKafkaOptions(final String type, final String appId, final String topic, final boolean retry) {
        try {
            String key = type + "~~" + appId + "~~" + topic;
            ACIKafkaOptions options = aciKafkaOptionsCache.peek(key);
            if (options != null) {
                LOGGER.debug("ACIKafkaOptions loaded successfully to cache for key {} {}", key, options);
                return options;
            }
            options = aciKafkaOptionsCache.computeIfAbsent(key, k -> {
                LOGGER.debug("ACIKafkaOptions not loaded in cache [ {} ~~ {} ~~ {} ]. Will load to cache now..", type, appId, topic);
                try {
                    ACIKafkaOptions opt = new ACIKafkaOptions();
                    if (isBlank(topic)) {
                        String publisherRegistrationUrl = environment.getConfig().getDhariUri() + "/restricted/publisher/fetch/" + DEFAULT_APP_TYPE + "/" + appId;
                        try {
                            opt = WebUtils.getJsonData(publisherRegistrationUrl, AppConfig.appId(), A3Utils.getA3Token(A3Constants.KNOWN_APP.DHARI.appId()), a3Mode(), ACIKafkaOptions.class);
                            if (opt == null) {
                                return null;
                            }
                        } catch (Exception e) {
                            LOGGER.error("Could not invoke the Dhari url : " + publisherRegistrationUrl, e);
                        }
                    }
                    Map defaults;
                    String fetchPublisherDefaultsUrl = environment.getConfig().getDhariUri() + "/restricted/publisher/defaults";
                    try {
                        defaults = WebUtils.getJsonData(fetchPublisherDefaultsUrl, AppConfig.appId(), A3Utils.getA3Token(A3Constants.KNOWN_APP.DHARI.appId()), a3Mode(), Map.class);
                        if (defaults == null || defaults.isEmpty()) {
                            throw new Exception("Defaults fetched from Dhari is null or empty");
                        }
                    } catch (Exception e) {
                        throw new Exception("Could not invoke the Dhari url : " + fetchPublisherDefaultsUrl, e);
                    }
                    Map overrides = pojoToProperties(opt, "");
                    if (defaults != null) {
                        defaults.putAll(overrides);
                    }
                    opt = propertiesToPojo(defaults, ACIKafkaOptions.class);
                    return opt;
                } catch (Exception e) {
                    LOGGER.debug("Could not get ACIKafkaOptions for " + pairsOf("type", type, IDMS_APP_ID, appId, TOPIC, topic, "retry", retry), e);
                    return null;
                }
            });
            if (options == null) {
                throw new Exception("could not get ACIKafkaOptions");
            }
            LOGGER.debug("ACIKafkaOptions loaded successfully to cache for key {} {}", key, options);
            return options;
        } catch (Exception e) {
            LOGGER.error("Could not setup configs for ACIKafkaOptions for " + pairsOf("type", type, IDMS_APP_ID, appId, TOPIC, topic, "retry", retry), e);
            if (retry) {
                return getACIKafkaOptions(type, appId, topic, false);
            }
            return null;
        }
    }

    public Mono<ResponseBody> createModelPipeline(final String appId, final ModelPipeline pipeline) {
        pipeline.setAppId(appId);
        if (pipeline.getInitDate() == null) {
            pipeline.setInitDate(new Date());
        }
        return Mono.create(sink -> {
            try {
                LOGGER.debug("Pipeline : {} ", pipeline);
                pipeline.validate();
                ACIKafkaOptions options = getACIKafkaOptions(DEFAULT_APP_TYPE, appId, null);
                options.setTopic(pipeline.getSrcTopic());
                pipeline.apiUri(options.getApiUri());
                pipeline.uri(options.getUri());
                pipeline.apiToken(options.getApiToken());
                LOGGER.debug("Initiate clone-aci-kafka-topic received for {} {} ", pairsOf(CALLER_APP_ID, appId, "pipeline", pipeline));
                String group = options.getGroup();
                String namespace = options.getNamespace();
                final boolean donotCreate = (pipeline.getAutoCreateTopic() == null || !pipeline.getAutoCreateTopic());
                ensureTopicWithProperAccess(options, pipeline, donotCreate, options.getSourceTopic());
                if (pipeline.isEnableDirectAccess()) {
                    pipeline.setTargetTopic(pipeline.getSrcTopic());
                    if (pipeline.isEnableEncryption()) {
                        // TODO : Work on it later
                    } else {
                        String targetPublicKey = pipeline.resolvedTargetPublicKey();
                        if (!isBlank(targetPublicKey)) {
                            String targetClientId = isBlank(pipeline.getTargetClientId()) ? pipeline.getSrcTopic() + MODEL_PIPELINE_CLIENT_ID_SUFFIX : pipeline.getTargetClientId();
                            ensureExternalClientWithProperAccess(options, pipeline, group, namespace, options.getSourceTopic(), pipeline.getSrcTopic(), targetPublicKey, targetClientId);
                        }
                    }
                } else {
                    //todo
                }
                sink.success(new ResponseBody("Pipeline of shuri model created successfully"));
            } catch (Exception e) {
                if (e instanceof AcceptedStateException) {
                    AcceptedStateException exception = (AcceptedStateException) e;
                    LOGGER.debug("Initiate clone-aci-kafka-topic in Accepted state. Details : {} {} {}", exception.getMessage(), pairsOf(CALLER_APP_ID, appId, "pipeline", pipeline));
                } else {
                    LOGGER.error("initiate clone-aci-kafka-topic received for " + pairsOf(CALLER_APP_ID, appId, "pipeline", pipeline), e);
                }
                sink.error(e);
            }
        });
    }

    private void ensureTopicWithProperAccess(final ACIKafkaOptions options, final Object pipeline, final boolean donotCreate, final ACIKafkaTopicOptions topicConfig) throws Exception {
        ACIPipeline aciPipeline = null;
        ModelPipeline modelPipeline = null;
        if (pipeline instanceof ACIPipeline) {
            aciPipeline = (ACIPipeline) pipeline;
        }
        if (pipeline instanceof ModelPipeline) {
            modelPipeline = (ModelPipeline) pipeline;
        }
        Map details;
        try {
            details = ACIKafkaRestClient.topicDetails(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getNamespace(), options.getTopic());
        } catch (Exception e) {
            throw new Exception("Could not find details for topic " + options.getTopic() + ". Reason : " + e.getMessage(), e);
        }
        if (details == null || details.isEmpty()) {
            if (donotCreate) {
                throw new Exception("Could not auto-create topic " + options.getTopic());
            }
            Map status;
            try {
                status = ACIKafkaRestClient.createTopic(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getNamespace(), options.getTopic(), topicConfig.restPostBody(options.getTopic()));
                if (status == null || status.isEmpty()) {
                    throw new Exception("Invalid createTopic response");
                }
            } catch (Exception e) {
                throw new Exception("Could not create new topic " + options.getTopic() + ". Reason : " + e.getMessage(), e);
            }
            String activityId = (String) status.get("id");
            if (!isBlank(activityId)) {
                if (pipeline instanceof ACIPipeline) {
                    aciPipeline.latestActivityId(activityId);
                    pipelineQueue.add(aciPipeline);
                } else if (pipeline instanceof ModelPipeline) {
                    modelPipeline.latestActivityId(activityId);
                    modelPipelineQueue.add(modelPipeline);
                }
            }
            throw new AcceptedStateException("Topic [" + options.getTopic() + "] does not exist. Hence raised request to create a new topic with ACI.", Map.of("activityId", activityId, "activityName", "createNewTopic"));
        }

        if (pipeline instanceof ACIPipeline) {
            Map clientDetails = ACIKafkaRestClient.clientDetails(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getClientId());
            if (clientDetails == null || clientDetails.isEmpty()) {
                Map status = ACIKafkaRestClient.createClient(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getClientId(), EncryptionUtils.toPEMPublicKey(aciPipeline.getPublicKey()));
                if (status == null || status.isEmpty()) {
                    throw new Exception("Could not create new client " + options.getClientId());
                }
                String activityId = (String) status.get("id");
                if (!isBlank(activityId)) {
                    aciPipeline.latestActivityId(activityId);
                    pipelineQueue.add(aciPipeline);
                }
                throw new AcceptedStateException("Client [" + options.getClientId() + "] does not exist. Hence raised request to create a new client with ACI.", Map.of("activity", activityId, "activityName", "createNewClient"));
            }
        }

        Map accessDetails;
        try {
            accessDetails = ACIKafkaRestClient.clientAccess(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getNamespace(), options.getTopic(), options.getClientId());
        } catch (Exception e) {
            throw new Exception("Could not find access details for topic " + options.getTopic() + " against client " + options.getClientId() + ". Reason : " + e.getMessage(), e);
        }
        Map status = null;
        boolean validate = false;
        String activityName = null;
        try {
            if (accessDetails == null || accessDetails.isEmpty() || !accessDetails.containsKey("entity")) {
                status = ACIKafkaRestClient.createProducerAndConsumerAccess(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getNamespace(), options.getTopic(), options.getClientId(), topicConfig.getProducerClientQuota(), topicConfig.getConsumerClientQuota());
                validate = true;
                activityName = "createProducerAndConsumerAccess";
            } else if (!((Map) accessDetails.get("entity")).containsKey("produce")) {
                status = ACIKafkaRestClient.updateProducerAccess(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getNamespace(), options.getTopic(), options.getClientId(), accessDetails, topicConfig.getProducerClientQuota());
                validate = true;
                activityName = "updateProducerAccess";
            } else if (!((Map) accessDetails.get("entity")).containsKey("consume")) {
                status = ACIKafkaRestClient.updateConsumerAccess(options.getApiUri(), options.getApiToken(), options.getGroup(), options.getNamespace(), options.getTopic(), options.getClientId(), accessDetails, topicConfig.getConsumerClientQuota());
                validate = true;
                activityName = "updateConsumerAccess";
            }
        } catch (Exception e) {
            throw new Exception("Could not provide producer & consumer access to topic " + options.getTopic() + " against client " + options.getClientId() + ". Reason : " + e.getMessage(), e);
        }
        if (validate) {
            if (status == null || status.isEmpty()) {
                throw new Exception("Could not give permissions for client " + options.getClientId() + " to target topic " + options.getTopic());
            }
            String activityId = (String) status.get("id");
            if (!isBlank(activityId)) {
                if (pipeline instanceof ACIPipeline) {
                    aciPipeline.latestActivityId(activityId);
                    pipelineQueue.add(aciPipeline);
                } else if (pipeline instanceof ModelPipeline) {
                    modelPipeline.latestActivityId(activityId);
                    modelPipelineQueue.add(modelPipeline);
                }
            }
            throw new AcceptedStateException("Client [" + options.getClientId() + "] does not have producer access to topic [" + options.getTopic() + "]. Hence raised request to provide access with ACI.", Map.of("activityId", activityId, "activityName", activityName));
        }
    }

    private void ensureExternalClientWithProperAccess(final ACIKafkaOptions options, final ModelPipeline pipeline, final String group, final String namespace, final ACIKafkaTopicOptions topicConfig, final String targetTopic, final String targetPublicKey, final String targetClientId) throws Exception {
        Map clientDetails = ACIKafkaRestClient.clientDetails(options.getApiUri(), options.getApiToken(), group, targetClientId);
        if (clientDetails == null || clientDetails.isEmpty()) {
            Map status = ACIKafkaRestClient.createClient(options.getApiUri(), options.getApiToken(), group, targetClientId, targetPublicKey);
            if (status == null || status.isEmpty()) {
                throw new Exception("Could not create new client " + targetClientId);
            }
            String activityId = (String) status.get("id");
            if (!isBlank(activityId)) {
                pipeline.latestActivityId(activityId);
                modelPipelineQueue.add(pipeline);
            }
            throw new AcceptedStateException("Client [" + targetClientId + "] does not exist. Hence raised request to create a new client with ACI.", Map.of("activity", activityId, "activityName", "createNewClient"));
        }
        Map accessDetails;
        try {
            accessDetails = ACIKafkaRestClient.clientAccess(options.getApiUri(), options.getApiToken(), group, namespace, targetTopic, targetClientId);
        } catch (Exception e) {
            throw new Exception("Could not find access details for topic " + targetTopic + " against client " + targetClientId + ". Reason : " + e.getMessage(), e);
        }
        Map status = null;
        boolean validate = false;
        String activityName = null;
        if (accessDetails == null || accessDetails.isEmpty() || !accessDetails.containsKey("entity")) {
            status = ACIKafkaRestClient.createConsumerAccess(options.getApiUri(), options.getApiToken(), group, namespace, targetTopic, targetClientId, topicConfig.getConsumerClientQuota());
            validate = true;
            activityName = "createConsumerAccess";
        } else if (!((Map) accessDetails.get("entity")).containsKey("consume")) {
            status = ACIKafkaRestClient.updateConsumerAccess(options.getApiUri(), options.getApiToken(), group, namespace, targetTopic, targetClientId, accessDetails, topicConfig.getConsumerClientQuota());
            validate = true;
            activityName = "updateConsumerAccess";
        }
        if (validate) {
            if (status == null || status.isEmpty()) {
                throw new Exception("Could not give permissions for newly created client " + targetClientId + " to target topic " + targetTopic);
            }
            String activityId = (String) status.get("id");
            if (!isBlank(activityId)) {
                pipeline.latestActivityId(activityId);
                modelPipelineQueue.add(pipeline);
            }
            throw new AcceptedStateException("Client [" + targetClientId + "] does not have consumer access to topic [" + targetTopic + "]. Hence raised request to provide access with ACI.", Map.of("activityId", activityId, "activityName", activityName));
        }
    }

    private void continueModelPipelineCreation(final ModelPipeline pipeline) {
        try {
            createModelPipeline(pipeline.getAppId(), pipeline).doOnSuccess(responseBody -> {
                String callBackUrl = pipeline.getCallBackUrl();
                if (isBlank(callBackUrl)) {
                    return;
                }
                Map<String, String> callBackStatus = Map.of("status", "SUCCESS");
                try {
                    WebUtils.postJsonData(callBackUrl, callBackStatus, AppConfig.appId(), A3Utils.getA3Token(parseLong(pipeline.getAppId())), a3Mode(), Map.class);
                } catch (Exception e) {
                    LOGGER.error("Could not invoke the callBackUrl : " + callBackUrl, e);
                }
            }).block();
        } catch (Exception ignored) {
            LOGGER.trace("Error while creating pipeline : {} ", pipeline);
        }
    }

    public Mono<ResponseBody> removeModelPipeline(final String appId, final ModelCleanupPipeline pipeline) {
        pipeline.setAppId(appId);
        if (pipeline.getInitDate() == null) {
            pipeline.setInitDate(new Date());
        }
        return Mono.create(sink -> {
            LOGGER.debug("Pipeline : {} ", pipeline);
            try {
                pipeline.validate();
                ACIKafkaOptions options = getACIKafkaOptions(DEFAULT_APP_TYPE, appId, null);
                options.setTopic(pipeline.getSrcTopic());
                LOGGER.debug("removeACIKafkaResources received for {}", pairsOf(CALLER_APP_ID, appId, "pipeline", pipeline));
                String group = options.getGroup();
                String namespace = options.getNamespace();
                String topic = options.getTopic();
                pipeline.apiUri(options.getApiUri());
                pipeline.uri(options.getUri());
                pipeline.apiToken(options.getApiToken());
                Map topicDetails;
                try {
                    topicDetails = ACIKafkaRestClient.topicDetails(options.getApiUri(), options.getApiToken(), group, namespace, topic);
                } catch (Exception e) {
                    throw new Exception("Could not find details for topic " + topic + " for removeACIKafkaResources from " + pairsOf(CALLER_APP_ID, appId), e);
                }
                if (topicDetails == null || topicDetails.isEmpty()) {
                    LOGGER.debug("No ACI Kafka resources for topic {}.", topic);
                } else {
                    if (pipeline.isEnableDirectAccess()) {
                        if (pipeline.isEnableEncryption()) {
                            // TODO : Work on it later
                        } else {
                            String targetClientId = isBlank(pipeline.getTargetClientId()) ? topic + MODEL_PIPELINE_CLIENT_ID_SUFFIX : pipeline.getTargetClientId();
                            deleteAccesses(pipeline, options, group, namespace, topic);
                            deleteClient(pipeline, options, group, targetClientId);
                            if (pipeline.getAutoCreateTopic() == null || !pipeline.getAutoCreateTopic()) {
                                throw new Exception("Could not delete topic " + topic + ", because it is not an auto created topic");
                            } else {
                                deleteTopic(pipeline, options, group, namespace, topic);
                            }
                        }
                    } else {
                        //TODO:
                    }
                    LOGGER.debug("ACI Kafka resources for shuri model cleaned up successfully for {} {}", pairsOf(CALLER_APP_ID, appId, "pipeline", pipeline));
                }
                sink.success(new ResponseBody("ACI Kafka resources for shuri model cleaned up successfully"));
            } catch (Exception e) {
                LOGGER.error("Failed to remove ACI Kafka resources " + pairsOf(CALLER_APP_ID, appId), e);
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void deleteAccesses(final Object pipeline, final ACIKafkaOptions options, final String group, final String namespace, final String topic) throws Exception {
        ACICleanupPipeline aciPipeline = null;
        ModelCleanupPipeline modelPipeline = null;
        if (pipeline instanceof ACICleanupPipeline) {
            aciPipeline = (ACICleanupPipeline) pipeline;
        }
        if (pipeline instanceof ModelCleanupPipeline) {
            modelPipeline = (ModelCleanupPipeline) pipeline;
        }
        Map accesses = ACIKafkaRestClient.topicAccessDetails(options.getApiUri(), options.getApiToken(), group, namespace, topic);
        if (accesses == null || accesses.isEmpty()) {
            LOGGER.debug("No topic accesses of topic : " + topic);
            return;
        }
        if (accesses.containsKey("entities")) {
            for (Map entity : (List<Map>) accesses.get("entities")) {
                if (entity.containsKey("id") && ((Map) entity.get("id")).containsKey("group") && ((Map) entity.get("id")).containsKey("identity")) {
                    Map status;
                    try {
                        status = ACIKafkaRestClient.deleteAccess(options.getApiUri(), options.getApiToken(), group, namespace, topic, (String) ((Map) entity.get("id")).get("group"), (String) ((Map) entity.get("id")).get("identity"));
                        if (status == null || status.isEmpty()) {
                            throw new Exception("Invalid deleteAccess response");
                        }
                    } catch (Exception e) {
                        throw new Exception("Could not delete access to topic " + topic + " against identity group " + ((Map) entity.get("id")).get("group") + " identity " + ((Map) entity.get("id")).get("identity") + ". Reason : " + e.getMessage(), e);
                    }
                    String activityId = (String) status.get("id");
                    if (!isBlank(activityId)) {
                        if (pipeline instanceof ACICleanupPipeline) {
                            aciPipeline.latestActivityId(activityId);
                            aciCleanupPipelineQueue.add(aciPipeline);
                        } else if (pipeline instanceof ModelCleanupPipeline) {
                            modelPipeline.latestActivityId(activityId);
                            modelCleanupPipelineQueue.add(modelPipeline);
                        }
                    }
                    throw new AcceptedStateException("Client Access [" + ((Map) entity.get("id")).get("identity") + "] has not been deleted. Hence raised request to delete accesses with ACI.", Map.of("activityId", activityId, "activityName", "deleteAccess"));
                }
            }
        }
    }

    private void deleteClient(final Object pipeline, final ACIKafkaOptions options, final String group, final String targetClientId) throws Exception {
        ACICleanupPipeline aciPipeline = null;
        ModelCleanupPipeline modelPipeline = null;
        if (pipeline instanceof ACICleanupPipeline) {
            aciPipeline = (ACICleanupPipeline) pipeline;
        }
        if (pipeline instanceof ModelCleanupPipeline) {
            modelPipeline = (ModelCleanupPipeline) pipeline;
        }
        Map clientDetails = ACIKafkaRestClient.clientDetails(options.getApiUri(), options.getApiToken(), group, targetClientId);
        if (clientDetails == null || clientDetails.isEmpty()) {
            LOGGER.debug("No clients of topic : " + options.getTopic());
            return;
        }
        Map deleteClientStatus;
        try {
            deleteClientStatus = ACIKafkaRestClient.deleteIdentity(options.getApiUri(), options.getApiToken(), group, targetClientId);
            if (deleteClientStatus == null || deleteClientStatus.isEmpty()) {
                throw new Exception("Invalid deleteIdentity response");
            }
        } catch (Exception e) {
            throw new Exception("Could not delete identity " + targetClientId + ". Reason : " + e.getMessage(), e);
        }
        String activityId = (String) deleteClientStatus.get("id");
        if (!isBlank(activityId)) {
            if (pipeline instanceof ACICleanupPipeline) {
                aciPipeline.latestActivityId(activityId);
                aciCleanupPipelineQueue.add(aciPipeline);
            } else if (pipeline instanceof ModelCleanupPipeline) {
                modelPipeline.latestActivityId(activityId);
                modelCleanupPipelineQueue.add(modelPipeline);
            }
        }
        throw new AcceptedStateException("Client [" + targetClientId + "] has not been deleted. Hence raised request to delete client with ACI.", Map.of("activityId", activityId, "activityName", "deleteClient"));
    }

    private void deleteTopic(final Object pipeline, final ACIKafkaOptions options, final String group, final String namespace, final String topic) throws Exception {
        ACICleanupPipeline aciPipeline = null;
        ModelCleanupPipeline modelPipeline = null;
        if (pipeline instanceof ACICleanupPipeline) {
            aciPipeline = (ACICleanupPipeline) pipeline;
        }
        if (pipeline instanceof ModelCleanupPipeline) {
            modelPipeline = (ModelCleanupPipeline) pipeline;
        }
        Map deleteTopicStatus;
        try {
            deleteTopicStatus = ACIKafkaRestClient.deleteTopic(options.getApiUri(), options.getApiToken(), group, namespace, topic);
            if (deleteTopicStatus == null || deleteTopicStatus.isEmpty()) {
                throw new Exception("Invalid deleteIdentity response");
            }
        } catch (Exception e) {
            throw new Exception("Could not delete topic " + topic + ". Reason : " + e.getMessage(), e);
        }
        String activityId = (String) deleteTopicStatus.get("id");
        if (!isBlank(activityId)) {
            if (pipeline instanceof ACICleanupPipeline) {
                aciPipeline.latestActivityId(activityId);
                aciCleanupPipelineQueue.add(aciPipeline);
            } else if (pipeline instanceof ModelCleanupPipeline) {
                modelPipeline.latestActivityId(activityId);
                modelCleanupPipelineQueue.add(modelPipeline);
            }
        }
        throw new AcceptedStateException("Topic [" + topic + "] has not been deleted. Hence raised request to delete topic with ACI.", Map.of("activityId", activityId, "activityName", "deleteTopic"));
    }

    private void continuePipelineCleanup(final ModelCleanupPipeline pipeline) {
        try {
            removeModelPipeline(pipeline.appId(), pipeline).doOnSuccess(responseBody -> {
                String callBackUrl = pipeline.getCallBackUrl();
                if (isBlank(callBackUrl)) {
                    return;
                }
                Map<String, String> callBackStatus = Map.of("status", "SUCCESS");
                try {
                    WebUtils.postJsonData(callBackUrl, callBackStatus, AppConfig.appId(), A3Utils.getA3Token(parseLong(pipeline.appId())), a3Mode(), Map.class);
                } catch (Exception e) {
                    LOGGER.error("Could not invoke the callBackUrl : " + callBackUrl, e);
                }
            }).block();
        } catch (Exception ignored) {
            LOGGER.trace("Error while cleanup pipeline : {} ", pipeline);
        }
    }

    private void continueTopicCleanup(final KafkaTopicCleanup topicCleanup) {
        try {
            removeKafkaTopic(topicCleanup);
        } catch (Exception ignored) {
            LOGGER.trace("Error while cleanup topic : {} ", topicCleanup);
        }
    }

    @SuppressWarnings("unchecked")
    public Mono<ResponseBody> removeKafkaTopic(final String appId, final String group, final String namespace, final String topic) {
        return Mono.create(sink -> {
            try {
                if (!isBlank(appId)) {
                    if (!appId.equals(String.valueOf(A3Constants.KNOWN_APP.STARGATE.appId()))) {
                        throw new UnauthorizedException("This is an internal endpoint for stargate use only");
                    }
                }
                LOGGER.debug("removeACIKafkaResources received for {}.{}.{}", group, namespace, topic);
                Map defaults;
                String fetchPublisherDefaultsUrl = environment.getConfig().getDhariUri() + "/restricted/publisher/defaults";
                try {
                    defaults = WebUtils.getJsonData(fetchPublisherDefaultsUrl, AppConfig.appId(), A3Utils.getA3Token(A3Constants.KNOWN_APP.DHARI.appId()), a3Mode(), Map.class);
                    if (defaults == null || defaults.isEmpty()) {
                        throw new Exception("Defaults fetched from Dhari is null or empty");
                    }
                    if (!defaults.containsKey("apiUri") || !defaults.containsKey("apiToken")) {
                        throw new Exception("Failed to fetch default value of apiUri or apiToken");
                    }
                } catch (Exception e) {
                    throw new Exception("Could not invoke the Dhari url : " + fetchPublisherDefaultsUrl, e);
                }
                KafkaTopicCleanup topicCleanup = new KafkaTopicCleanup(group, namespace, topic, ((String) defaults.get("apiUri")), ((String) defaults.get("apiToken")));
                removeKafkaTopic(topicCleanup);
            } catch (Exception e) {
                LOGGER.error("Failed to remove ACI Kafka resources ", e);
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void removeKafkaTopic(final KafkaTopicCleanup topicCleanup) throws Exception {
        Map topicDetails;
        try {
            topicDetails = ACIKafkaRestClient.topicDetails(topicCleanup.getApiUri(), topicCleanup.getApiToken(), topicCleanup.getGroup(), topicCleanup.getNamespace(), topicCleanup.getTopic());
        } catch (Exception e) {
            throw new Exception("Could not find details for topic " + topicCleanup.getTopic() + " for removeACIKafkaResources ", e);
        }
        if (topicDetails == null || topicDetails.isEmpty()) {
            LOGGER.debug("No ACI Kafka resources for topic {}.", topicCleanup.getTopic());
        } else {
            Map accesses = ACIKafkaRestClient.topicAccessDetails(topicCleanup.getApiUri(), topicCleanup.getApiToken(), topicCleanup.getGroup(), topicCleanup.getNamespace(), topicCleanup.getTopic());
            if (accesses != null && !accesses.isEmpty() && accesses.containsKey("entities")) {
                for (Map entity : (List<Map>) accesses.get("entities")) {
                    if (entity.containsKey("id") && ((Map) entity.get("id")).containsKey("group") && ((Map) entity.get("id")).containsKey("identity")) {
                        if (!((Map) entity.get("id")).get("identity").equals(appDefaults.getKafkaDefaultClientId())) {
                            topicCleanup.addClient((String) ((Map) entity.get("id")).get("identity"));
                        }
                        Map status;
                        try {
                            status = ACIKafkaRestClient.deleteAccess(topicCleanup.getApiUri(), topicCleanup.getApiToken(), topicCleanup.getGroup(), topicCleanup.getNamespace(), topicCleanup.getTopic(), (String) ((Map) entity.get("id")).get("group"), (String) ((Map) entity.get("id")).get("identity"));
                            if (status == null || status.isEmpty()) {
                                throw new Exception("Invalid deleteAccess response");
                            }
                        } catch (Exception e) {
                            throw new Exception("Could not delete access to topic " + topicCleanup.getTopic() + " against identity group " + ((Map) entity.get("id")).get("group") + " identity " + ((Map) entity.get("id")).get("identity") + ". Reason : " + e.getMessage(), e);
                        }
                        String activityId = (String) status.get("id");
                        if (!isBlank(activityId)) {
                            topicCleanup.setLatestActivityId(activityId);
                            kafkaTopicCleanupQueue.add(topicCleanup);
                        }
                        throw new AcceptedStateException("Client Access [" + ((Map) entity.get("id")).get("identity") + "] has not been deleted. Hence raised request to delete accesses with ACI.", Map.of("activityId", activityId, "activityName", "deleteAccess"));
                    }
                }
            }
            if (topicCleanup.getClients() != null) {
                for (final String clientId : topicCleanup.getClients()) {
                    if (clientId.equals(appDefaults.getKafkaDefaultClientId())) { //TBD: Is `shuri-dhari` client the only exception
                        continue;
                    }
                    Map clientDetails = ACIKafkaRestClient.clientDetails(topicCleanup.getApiUri(), topicCleanup.getApiToken(), topicCleanup.getGroup(), clientId);
                    if (clientDetails == null || clientDetails.isEmpty()) {
                        LOGGER.debug("No client : {} of topic : {}", clientId, topicCleanup.getTopic());
                    }
                    Map deleteClientStatus;
                    try {
                        deleteClientStatus = ACIKafkaRestClient.deleteIdentity(topicCleanup.getApiUri(), topicCleanup.getApiToken(), topicCleanup.getGroup(), clientId);
                        if (deleteClientStatus == null || deleteClientStatus.isEmpty()) {
                            throw new Exception("Invalid deleteIdentity response");
                        }
                    } catch (Exception e) {
                        throw new Exception("Could not delete identity " + clientId + ". Reason : " + e.getMessage(), e);
                    }
                    String activityId = (String) deleteClientStatus.get("id");
                    if (!isBlank(activityId)) {
                        topicCleanup.setLatestActivityId(activityId);
                        kafkaTopicCleanupQueue.add(topicCleanup);
                    }
                    topicCleanup.getClients().remove(clientId);
                    throw new AcceptedStateException("Client [" + clientId + "] has not been deleted. Hence raised request to delete client with ACI.", Map.of("activityId", activityId, "activityName", "deleteClient"));
                }
            }
            Map deleteTopicStatus;
            try {
                deleteTopicStatus = ACIKafkaRestClient.deleteTopic(topicCleanup.getApiUri(), topicCleanup.getApiToken(), topicCleanup.getGroup(), topicCleanup.getNamespace(), topicCleanup.getTopic());
                if (deleteTopicStatus == null || deleteTopicStatus.isEmpty()) {
                    throw new Exception("Invalid deleteIdentity response");
                }
            } catch (Exception e) {
                throw new Exception("Could not delete topic " + topicCleanup.getTopic() + ". Reason : " + e.getMessage(), e);
            }
            String activityId = (String) deleteTopicStatus.get("id");
            if (!isBlank(activityId)) {
                topicCleanup.setLatestActivityId(activityId);
                kafkaTopicCleanupQueue.add(topicCleanup);
            }
            throw new AcceptedStateException("Topic [" + topicCleanup.getTopic() + "] has not been deleted. Hence raised request to delete topic with ACI.", Map.of("activityId", activityId, "activityName", "deleteTopic"));
        }
        LOGGER.debug("ACI Kafka topic : {} cleaned up successfully", topicCleanup.getTopic());
    }

}
