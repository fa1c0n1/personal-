package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.service.DhariProxyService;
import com.apple.appeng.aluminum.auth.spring.security.reactive.AuthenticatedPrincipalProvider;
import io.micrometer.core.annotation.Timed;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.constants.CommonConstants.API_PREFIX;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static java.lang.Long.parseLong;

@Timed
@RestController
@RequestMapping(API_PREFIX + "/dhari")
//@Api(value = "Dhari Proxy Router")
public class DhariProxyRouter {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    private AuthenticatedPrincipalProvider authProvider;
    @Autowired
    private DhariProxyService dhariProxyService;

    @GetMapping(value = "/consume/{publisherId}/{noOfMessages}")
    //@ApiOperation(value = "Consume the last n messages from a given Dhari Publisher", response = ConsumerRecords.class)
    public Mono<String> consumeMessages(@PathVariable final String publisherId, @PathVariable final long noOfMessages, @RequestHeader(value = "X-Publisher-AppId", required = false, defaultValue = "-1") final long publisherAppId, @RequestHeader(value = "X-Kafka-Topic", required = false) final String topic, @RequestHeader(value = "X-Kafka-Partition", required = false, defaultValue = "-1") final int partition, @RequestHeader(value = "X-Kafka-Offset", required = false, defaultValue = "-1") final int offset, @RequestHeader(value = "X-Schema-Id", required = false) final String filterSchemaId, @RequestHeader(value = "X-Publish-Format", required = false) final String publishFormat, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> dhariProxyService.consumeMessagesAsJsonString(appId, publisherAppId, publisherId, noOfMessages, topic, partition, offset, filterSchemaId, publishFormat, request));
    }
}
