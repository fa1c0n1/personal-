package com.apple.aml.stargate.k8s.routes;

import com.apple.aml.stargate.common.configs.VersionInfo;
import com.apple.aml.stargate.common.spring.service.PrometheusService;
import com.apple.aml.stargate.k8s.service.CoreService;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import static com.apple.aml.stargate.common.constants.PipelineConstants.VERSION_INFO;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getServerVersion;
import static io.prometheus.client.exporter.common.TextFormat.CONTENT_TYPE_004;

@Timed
@RestController
@RequestMapping("/")
public class CoreRouter {
    @Autowired
    private CoreService service;
    @Autowired
    @Lazy
    private PrometheusService prometheusService;

    @GetMapping(value = "metrics", produces = CONTENT_TYPE_004)
    public Mono<String> metrics() {
        return prometheusService.metrics();
    }

    @GetMapping("favicon.ico")
    @ResponseBody
    void nofavicon() {
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

    @GetMapping(value = "version/server")
    public Mono<VersionInfo> serverVersionInfo(final ServerHttpRequest request) {
        return Mono.just(getServerVersion());
    }

    @PostMapping(value = "version/server")
    public Mono<VersionInfo> serverVersionInfoViaPost(final ServerHttpRequest request) {
        return Mono.just(getServerVersion());
    }

    @GetMapping(value = "version/k8s")
    public Mono<io.fabric8.kubernetes.client.VersionInfo> k8sVersionInfo(final ServerHttpRequest request) {
        return service.k8sVersionInfo();
    }

    @PostMapping(value = "version/k8s")
    public Mono<io.fabric8.kubernetes.client.VersionInfo> k8sVersionInfoViaPost(final ServerHttpRequest request) {
        return service.k8sVersionInfo();
    }
}
