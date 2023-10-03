package com.apple.aml.stargate.spark.facade.routes;

import com.apple.aml.stargate.common.configs.VersionInfo;
import com.apple.aml.stargate.common.spring.service.PrometheusService;
import com.apple.aml.stargate.spark.facade.service.SFCoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import static com.apple.aml.stargate.common.constants.PipelineConstants.VERSION_INFO;
import static io.prometheus.client.exporter.common.TextFormat.CONTENT_TYPE_004;

@RestController
@RequestMapping("/")
public class SFCoreRouter {
    @Autowired
    private PrometheusService prometheusService;
    @Autowired
    private SFCoreService service;

    @GetMapping(value = "metrics", produces = CONTENT_TYPE_004)
    public Mono<String> metrics() {
        return prometheusService.metrics();
    }

    @GetMapping("favicon.ico")
    @ResponseBody
    public void nofavicon() {
    }

    @GetMapping(value = "health")
    public Mono<String> health(final ServerHttpRequest request) {
        return service.readiness();
    }

    @GetMapping(value = "readiness")
    public Mono<String> readiness(final ServerHttpRequest request) {
        return service.readiness();
    }

    @GetMapping(value = "liveness")
    public Mono<String> liveness(final ServerHttpRequest request) {
        return service.liveness();
    }

    @GetMapping(value = "version")
    public Mono<VersionInfo> versionInfo(final ServerHttpRequest request) {
        return Mono.just(VERSION_INFO);
    }

    @PostMapping(value = "version")
    public Mono<VersionInfo> versionInfoViaPost(final ServerHttpRequest request) {
        return Mono.just(VERSION_INFO);
    }

    @PostMapping(value = "sql")
    public Flux<Map> fireSQL(final ServerHttpRequest request, @RequestBody String sql) {
        return service.fireSQL(request, sql);
    }

}
