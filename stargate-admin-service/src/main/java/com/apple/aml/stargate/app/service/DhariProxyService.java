package com.apple.aml.stargate.app.service;

import com.apple.aml.stargate.app.config.AppDefaults;
import com.apple.aml.stargate.app.pojo.Subscriber;
import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.BadRequestException;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.AppConfig;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.KafkaConstants.DEFAULT_APP_TYPE;
import static com.apple.aml.stargate.common.utils.A3Utils.a3Mode;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.WebUtils.getJsonData;
import static com.apple.jvm.commons.util.Strings.isBlank;

@Service
public class DhariProxyService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final PipelineConstants.ENVIRONMENT environment = AppConfig.environment();
    @Autowired
    private InfraService infraService;
    @Autowired
    private AppDefaults appDefaults;

    public Mono<String> consumeMessagesAsJsonString(final long appId, final long iPublisherAppId, final String consumerId, final long noOfMessages, final String iTopic, final int iPartition, final long offset, final String filterSchemaId, final String publishFormat, final ServerHttpRequest request) {
        return consumeMessages(appId, iPublisherAppId, consumerId, noOfMessages, iTopic, iPartition, offset, filterSchemaId, publishFormat, request).map(messages -> "[" + messages.stream().map(Object::toString).collect(Collectors.joining(",")) + "]");
    }

    public Mono<List<GenericRecord>> consumeMessages(final long appId, final long iPublisherAppId, final String consumerId, final long noOfMessages, final String iTopic, final int iPartition, final long offset, final String filterSchemaId, final String publishFormat, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            Map<String, Object> logHeaders = infraService.logHeaders(request);
            try {
                long publisherAppId;
                if (iPublisherAppId <= 0 || appId == iPublisherAppId) {
                    publisherAppId = appId;
                } else if (appId == A3Constants.KNOWN_APP.DHARI.appId() || appId == A3Constants.KNOWN_APP.STARGATE.appId()) {
                    publisherAppId = iPublisherAppId;
                } else {
                    throw new BadRequestException("Cannot serve cross-application appIds");
                }
                String publisherUri = environment.getConfig().getDhariUri() + "/restricted/publisher/fetch/" + DEFAULT_APP_TYPE + "/" + publisherAppId + "/" + consumerId;
                try (Subscriber subscriber = getJsonData(publisherUri, appId(), A3Utils.getA3Token(A3Constants.KNOWN_APP.DHARI.appId()), a3Mode(), Subscriber.class)) {
                    // TODO : We need to enforce that only selected few appId's can invoke selected few publisherAppId's
                    if (subscriber == null) {
                        LOGGER.error("Failed to create a subscriber from publisherAppId {}", publisherAppId);
                        throw new Exception("Failed to create a subscriber from publisherAppId {}");
                    }
                    subscriber.setSubscriberId(consumerId);
                    subscriber.init(null, null, isBlank(iTopic) ? consumerId : iTopic);
                    List<GenericRecord> messages = subscriber.consumeMessagesBySchemaId(filterSchemaId, Math.max(iPartition, -1), offset, noOfMessages, appDefaults.getConsumerPollDuration() * 1000);
                    sink.success(messages);
                } catch (Exception e) {
                    LOGGER.error("Failed to consume messages", logHeaders, e);
                }
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }
}
