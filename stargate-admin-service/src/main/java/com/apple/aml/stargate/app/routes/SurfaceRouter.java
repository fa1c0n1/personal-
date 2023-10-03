package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.service.PipelineService;
import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.appeng.aluminum.auth.spring.security.reactive.AuthenticatedPrincipalProvider;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import static com.apple.aml.stargate.common.constants.CommonConstants.API_PREFIX;
import static java.lang.Long.parseLong;

@Timed
@RestController
@RequestMapping(API_PREFIX + "/aiml")
public class SurfaceRouter {

    @Autowired
    private PipelineService pipelineService;
    @Autowired
    private AuthenticatedPrincipalProvider authProvider;

    @PostMapping(value = "/deployment/create/{pipelineId}")
    public Mono<ResponseBody> deployDataPipeline(@PathVariable final String pipelineId, @RequestBody final String payload, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.createDeployment(appId, pipelineId, payload, request));
    }

    @GetMapping(value = "/deployment/delete/{pipelineId}")
    public Mono<ResponseBody> deleteDataPipeline(@PathVariable final String pipelineId, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.deleteDeployment(appId, pipelineId, request));
    }

    @GetMapping(value = "/deployment/status/{pipelineId}")
    public Mono<ResponseBody> getDataPipelineStatus(@PathVariable final String pipelineId, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getDeploymentStatus(appId, pipelineId, request));
    }

}
