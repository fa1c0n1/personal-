package com.apple.aml.stargate.rm.routes;

import com.apple.aml.stargate.common.configs.VersionInfo;
import com.apple.aml.stargate.rm.service.RMCoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

import static com.apple.aml.stargate.common.constants.PipelineConstants.VERSION_INFO;
import static io.prometheus.client.exporter.common.TextFormat.CONTENT_TYPE_004;

@RestController
@RequestMapping("/")
public class RMCoreRouter {
    @Autowired
    private RMCoreService service;

    @GetMapping(value = "metrics", produces = CONTENT_TYPE_004)
    public Mono<String> metrics() {
        return service.metrics();
    }

    @GetMapping("favicon.ico")
    @ResponseBody
    void nofavicon() {
    }

    @GetMapping(value = "health")
    public Mono<String> health(final ServerHttpRequest request) {
        return Mono.just("OK");
    }

    @GetMapping(value = "version")
    public Mono<VersionInfo> versionInfo(final ServerHttpRequest request) {
        return Mono.just(VERSION_INFO);
    }

    @GetMapping(value = "status")
    public Mono<String> pipelineStatus(final ServerHttpRequest request) {
        return service.pipelineStatus(request);
    }

    @GetMapping(value = "resource/{path}")
    public Mono<ResponseEntity<Resource>> getSharedFile(@PathVariable final String path, final ServerHttpRequest request) {
        return service.getSharedFile(path, request);
    }

    @PostMapping(value = "state/save/{stateId}")
    public Mono<Object> saveState(@PathVariable final String stateId, @RequestBody Object state, final ServerHttpRequest request) {
        return service.saveState(stateId, state, request);
    }

    @PostMapping(value = "state/get/{stateId}")
    public Mono<Object> getState(@PathVariable final String stateId, final ServerHttpRequest request) {
        return service.getState(stateId, request);
    }

    @PostMapping(value = "schema/save/{schemaId}")
    public Mono<Map> saveSchema(@PathVariable final String schemaId, @RequestBody Object schema, final ServerHttpRequest request) {
        return service.saveSchema(schemaId, schema, request);
    }

    @PostMapping(value = "schema/get/{schemaId}")
    public Mono<Map> getSchema(@PathVariable final String schemaId, final ServerHttpRequest request) {
        return service.getSchema(schemaId, request);
    }
}
