package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.service.QueryService;
import com.apple.appeng.aluminum.auth.spring.security.reactive.AuthenticatedPrincipalProvider;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.API_PREFIX;
import static java.lang.Long.parseLong;

@Timed
@RestController
@RequestMapping(API_PREFIX + "/query")
public class QueryRouter {
    @Autowired
    private AuthenticatedPrincipalProvider authProvider;
    @Autowired
    private QueryService queryService;

    @PostMapping(value = "/eai")
    public Mono<List<Map<String, Object>>> getEAIMetadataQueryResults(@RequestBody final String query, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> queryService.getEAIMetadataQueryResults(appId, query, request));
    }
}
