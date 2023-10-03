package com.apple.aml.stargate.app.service;

import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.CommonConstants;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.constants.CommonConstants.ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.SAVED_ON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MongoInternalKeys.STATE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_TOKEN;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

@Service
public class StateService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final String COLLECTION_NAME = "checkpoint";
    private Cache<String, String> pipelineTokenCache = new Cache2kBuilder<String, String>() {
    }.entryCapacity(100).expireAfterWrite(5, TimeUnit.MINUTES).build();

    @Autowired
    private MongoDatabase db;

    @Autowired
    private PipelineService pipelineService;

    @SuppressWarnings("unchecked")
    public Mono<ResponseBody> saveState(final String pipelineId, final String pipelineToken, final String a3Token, final String stateId, final Map<String, Object> payload, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateToken(pipelineId, pipelineToken, a3Token);
                Instant now = Instant.now();
                Document doc = new Document();
                doc.putAll(payload);
                doc.put(PIPELINE_ID, pipelineId);
                doc.put(STATE_ID, stateId);
                doc.put(SAVED_ON, now);
                collection().insertOne(doc).subscribe(new Subscriber<>() {
                    private String id = null;

                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(InsertOneResult result) {
                        id = result.getInsertedId().asObjectId().getValue().toString();
                    }

                    @Override
                    public void onError(Throwable t) {
                        sink.error(t);
                    }

                    @Override
                    public void onComplete() {
                        if (id == null) {
                            sink.error(new GenericException("Could not save state", payload).wrap());
                        } else {
                            sink.success(new ResponseBody("State saved successfully", Map.of(ID, id)));
                        }
                    }
                });
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public void validateToken(final String pipelineId, final String pipelineToken, final String a3Token) {
        if (isBlank(a3Token) && isBlank(pipelineToken)) throw new SecurityException("Missing Security token. A3 Token or Pipeline Token Required");
        String token = isBlank(pipelineToken) ? null : pipelineToken.replaceAll("\"", CommonConstants.EMPTY_STRING);
        if (token != null && token.equals(pipelineTokenCache.get(pipelineId))) return;

        Document pipelineInfo;
        try {
            pipelineInfo = pipelineService.getPipelineDefinitionDoc(A3Constants.KNOWN_APP.STARGATE.appId(), pipelineId);
            if (pipelineInfo.containsKey("pipelineToken")) {
                String dbToken = pipelineInfo.getString("pipelineToken");
                pipelineTokenCache.put(pipelineId, dbToken);
                if (token != null && token.equals(dbToken)) return;
            }
        } catch (Exception e) {
            throw new SecurityException("Pipeline not found");
        }
        if (isBlank(a3Token)) throw new SecurityException("Invalid Pipeline token");
        long invokerAppId = A3Constants.KNOWN_APP.STARGATE.appId();
        try {
            if (pipelineInfo.getInteger("appId") != null && pipelineInfo.getInteger("appId") > 0) invokerAppId = pipelineInfo.getInteger("appId").longValue();
        } catch (Exception e) {
            LOGGER.warn("Error getting App Id from pipelineid so defaulted to Stargate App Id for validation", Map.of("pipelineid", pipelineId));
        }
        if (A3Utils.validate(invokerAppId, a3Token)) return;
        else throw new SecurityException("Invalid A3 token");
    }

    private MongoCollection<Document> collection() {
        return db.getCollection(COLLECTION_NAME);
    }

    @SuppressWarnings("unchecked")
    public static boolean isValidPipeline(final String pipelineId, final String pipelineToken) {
        try {
            return parseBoolean(WebUtils.getData(environment().getConfig().getDhariUri() + "/sg/pipeline/validate/" + pipelineId, Map.of(HEADER_PIPELINE_TOKEN, pipelineToken), String.class, true));
        } catch (Exception e) {
            return false;
        }
    }

    public Mono<List<Map>> getFullState(final String pipelineId, final String pipelineToken, final String a3Token, final String stateId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateToken(pipelineId, pipelineToken, a3Token);
                collection().find(and(eq(PIPELINE_ID, pipelineId), eq(STATE_ID, stateId))).subscribe(new Subscriber<>() {
                    List<Map> results = new ArrayList<>();

                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Document document) {
                        results.add(document);
                    }

                    @Override
                    public void onError(Throwable t) {
                        sink.error(t);
                    }

                    @Override
                    public void onComplete() {
                        sink.success(results);
                    }
                });
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }
}
