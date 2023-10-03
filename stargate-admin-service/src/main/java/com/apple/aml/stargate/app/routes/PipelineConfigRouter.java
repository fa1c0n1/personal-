package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.service.PipelineService;
import com.apple.appeng.aluminum.auth.spring.security.reactive.AuthenticatedPrincipalProvider;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

import static java.lang.Long.parseLong;

@Timed
@RestController
@RequestMapping("/sg")
public class PipelineConfigRouter {
    @Autowired
    private AuthenticatedPrincipalProvider authProvider;
    @Autowired
    private PipelineService pipelineService;

    @GetMapping(value = "/pipeline/props/{pipelineId}")
    public Mono<Map<String, String>> getPipelineProperties(@PathVariable final String pipelineId, final ServerHttpRequest request) throws Exception {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getPipelineProperties(appId, pipelineId, request));
    }

    @GetMapping(value = "/pipeline/spec/{pipelineId}")
    public Mono<String> getPipelineSpec(@PathVariable final String pipelineId, final ServerHttpRequest request) throws Exception {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getPipelineSpec(appId, pipelineId, request));
    }

    @GetMapping(value = "/pipeline/connect-info/{pipelineId}/{connectId}")
    public Mono<Map<String, String>> getConnectInfo(@PathVariable final String pipelineId, @PathVariable final String connectId, final ServerHttpRequest request) throws Exception {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getConnectInfo(appId, pipelineId, connectId, request));
    }

    @GetMapping(value = "/pipeline/connect-info-appconfig/{pipelineId}/{connectId}")
    public Mono<Map<String, String>> getConnectInfoFromAppConfig(@PathVariable final String pipelineId, @PathVariable final String connectId, final ServerHttpRequest request) throws Exception {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getConnectInfoFromAppConfig(appId, pipelineId, connectId, request));
    }

}
