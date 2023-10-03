package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.pojo.GithubPathDetails;
import com.apple.aml.stargate.app.pojo.PipelineMetadata;
import com.apple.aml.stargate.app.service.PipelineService;
import com.apple.aml.stargate.app.service.StateService;
import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.appeng.aluminum.auth.spring.security.reactive.AuthenticatedPrincipalProvider;
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

import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.API_PREFIX;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_A3_TOKEN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_TOKEN;
import static java.lang.Long.parseLong;

@Timed
@RestController
@RequestMapping(API_PREFIX + "/pipeline")
public class PipelineRouter {
    @Autowired
    private StateService stateService;
    @Autowired
    private PipelineService pipelineService;
    @Autowired
    private AuthenticatedPrincipalProvider authProvider;

    @PostMapping(value = "/state/save/{pipelineId}/{stateId}")
    public Mono<ResponseBody> saveState(@PathVariable final String pipelineId, @RequestHeader(value = HEADER_PIPELINE_TOKEN, required = false) final String pipelineToken, @RequestHeader(value = HEADER_A3_TOKEN, required = false) final String a3Token, @PathVariable final String stateId, @RequestBody final Map<String, Object> payload, final ServerHttpRequest request) {
        return stateService.saveState(pipelineId, pipelineToken, a3Token, stateId, payload, request);
    }

    @GetMapping(value = "/state/get/{pipelineId}/{stateId}")
    public Mono<List<Map>> getFullState(@PathVariable final String pipelineId, @RequestHeader(value = HEADER_PIPELINE_TOKEN, required = false) final String pipelineToken, @RequestHeader(value = HEADER_A3_TOKEN, required = false) final String a3Token, @PathVariable final String stateId, final ServerHttpRequest request) {
        return stateService.getFullState(pipelineId, pipelineToken, a3Token, stateId, request);
    }

    @PostMapping(value = "/deployment/register/{pipelineId}")
    public Mono<ResponseBody> registerDataPipeline(@PathVariable final String pipelineId, @RequestBody final String payload, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.createDslDataPipeline(appId, pipelineId, payload, request));
    }

    @PostMapping(value = "/deployment/register/{pipelineId}/github")
    public Mono<ResponseBody> registerGithubDataPipeline(@PathVariable final String pipelineId, @RequestBody final GithubPathDetails details, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.createGithubDataPipeline(appId, pipelineId, details, request));
    }

    @PostMapping(value = "/deployment/manifest/{pipelineId}")
    public Mono<String> getStargateK8sManifest(@PathVariable final String pipelineId, @RequestBody(required = false) final Map<String, Object> overrides, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getStargateK8sManifest(appId, null, pipelineId, overrides, false, request));
    }

    @PostMapping(value = "/deployment/run-info/{pipelineId}")
    public Mono<Map<String, Object>> getLatestRunInfo(@PathVariable final String pipelineId, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getLatestRunInfo(appId, pipelineId, request));
    }

    @PostMapping(value = "/deployment/get/app/{runner}/{pipelineId}")
    public Mono<String> getStargateK8sManifestForRunner(@PathVariable final String runner, @PathVariable final String pipelineId, @RequestBody(required = false) final Map<String, Object> overrides, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getStargateK8sManifest(appId, runner, pipelineId, overrides, false, request));
    }

    @PostMapping(value = "/deployment/get/app/{runner}/{pipelineId}/local")
    public Mono<String> getStargateDebugK8sManifestForRunner(@PathVariable final String runner, @PathVariable final String pipelineId, @RequestBody(required = false) final Map<String, Object> overrides, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getStargateK8sManifest(appId, runner, pipelineId, overrides, true, request));
    }

    @PostMapping(value = "/deployment/get/app/token/{pipelineId}")
    public Mono<String> getPipelineToken(@PathVariable final String pipelineId, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getPipelineToken(appId, pipelineId, request));
    }

    @Deprecated
    @PostMapping(value = "/deployment/create") // Meant only for backward compatibility; Will be deprecated soon
    public Mono<ResponseBody> createDataPipeline(@RequestBody final String payload, @RequestHeader(value = "X-Payload-Separator", required = false) final String separator, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.createDataPipeline(appId, payload, separator, request));
    }

    @Deprecated
    @PostMapping(value = "/deployment/create/flink") // Meant only for backward compatibility; Will be deprecated soon
    public Mono<ResponseBody> createFlinkDataPipeline(@RequestBody final String payload, @RequestHeader(value = "X-Payload-Separator", required = false) final String separator, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.createDataPipeline(appId, payload, separator, request));
    }

    @PostMapping(value = "/deployment/set/app/metadata/{pipelineId}")
    public Mono<Map<String, Object>> setPipelineMetadataViaA3(@PathVariable final String pipelineId, @RequestBody(required = true) final PipelineMetadata overrides, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.setPipelineMetadata(pipelineId, overrides, request));
    }

    @GetMapping(value = "/deployment/get/app/pipelineInfo/{pipelineId}")
    public Mono<String> getPipelineInfoViaA3(@PathVariable final String pipelineId, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getPipelineInfo(pipelineId));
    }

    @GetMapping(value = "/deployment/pipelineToken/{pipelineId}")
    public Mono<String> getPipelineTokenViaA3(@PathVariable final String pipelineId, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> pipelineService.getOrCreatePipelineToken(appId, pipelineId));
    }

}
