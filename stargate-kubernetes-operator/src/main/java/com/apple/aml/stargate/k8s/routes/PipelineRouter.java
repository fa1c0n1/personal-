package com.apple.aml.stargate.k8s.routes;

import com.apple.aml.stargate.k8s.service.CoreService;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_A3_TOKEN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_APP_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_SHARED_TOKEN;

@Timed
@RestController
@RequestMapping("/sg")
public class PipelineRouter {
    @Autowired
    private CoreService coreService;

    @GetMapping(value = "/pipeline/props/{pipelineId}")
    public Mono<Map<String, String>> getPipelineProperties(@PathVariable final String pipelineId, @RequestHeader(value = HEADER_PIPELINE_APP_ID) final long appId, @RequestHeader(value = HEADER_PIPELINE_SHARED_TOKEN) final String token, final ServerHttpRequest request) throws Exception {
        return coreService.getPipelineProperties(appId, pipelineId, token, request);
    }

    @GetMapping(value = "/pipeline/spec/{pipelineId}")
    public Mono<String> getPipelineSpec(@PathVariable final String pipelineId, @RequestHeader(value = HEADER_PIPELINE_APP_ID) final long appId, @RequestHeader(value = HEADER_PIPELINE_SHARED_TOKEN) final String token, final ServerHttpRequest request) throws Exception {
        return coreService.getPipelineSpec(appId, pipelineId, token, request);
    }

    @GetMapping(value = "/pipeline/connect-info/{pipelineId}/{connectId}")
    public Mono<Map<String, String>> getConnectInfo(@PathVariable final String pipelineId, @PathVariable final String connectId, @RequestHeader(value = HEADER_PIPELINE_APP_ID) final long appId, @RequestHeader(value = HEADER_PIPELINE_SHARED_TOKEN) final String token, final ServerHttpRequest request) throws Exception {
        return coreService.getConnectInfo(appId, pipelineId, token, connectId, request);
    }

    @GetMapping(value = "/pipeline/get/connect-info/{connectId}")
    public Mono<Map<String, String>> getLocalConnectInfo(@PathVariable final String connectId, @RequestHeader(value = HEADER_A3_TOKEN) final String token, final ServerHttpRequest request) throws Exception {
        return coreService.getLocalConnectInfo(token, connectId, request);
    }

    @PostMapping(value = "/pipeline/save/connect-info/{connectId}")
    public Mono<Map<String, Object>> saveConnectInfoLocally(@PathVariable final String connectId, @RequestBody final Map<String, Object> connectInfo, @RequestHeader(value = HEADER_A3_TOKEN) final String token, final ServerHttpRequest request) throws Exception {
        return coreService.saveConnectInfoLocallyAsSecret(token, connectId, connectInfo, request);
    }

    @PostMapping(value = "/pipeline/save/connect-secret/{connectId}")
    public Mono<Map<String, Object>> saveConnectInfoLocallyAsSecret(@PathVariable final String connectId, @RequestBody final Map<String, Object> connectInfo, @RequestHeader(value = HEADER_A3_TOKEN) final String token, final ServerHttpRequest request) throws Exception {
        return coreService.saveConnectInfoLocallyAsSecret(token, connectId, connectInfo, request);
    }

    @PostMapping(value = "/pipeline/save/connect-configmap/{connectId}")
    public Mono<Map<String, Object>> saveConnectInfoLocallyAsConfigMap(@PathVariable final String connectId, @RequestBody final Map<String, Object> connectInfo, @RequestHeader(value = HEADER_A3_TOKEN) final String token, final ServerHttpRequest request) throws Exception {
        return coreService.saveConnectInfoLocallyAsConfigMap(token, connectId, connectInfo, request);
    }
}
