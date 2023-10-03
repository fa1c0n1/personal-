package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.service.InfraService;
import com.apple.aml.stargate.common.configs.VersionInfo;
import com.apple.aml.stargate.common.spring.service.PrometheusService;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import static com.apple.aml.stargate.common.constants.PipelineConstants.VERSION_INFO;
import static io.prometheus.client.exporter.common.TextFormat.CONTENT_TYPE_004;

@Timed
@RestController
@RequestMapping("/")
//@Api(value = "Infra Service Router")
public class InfraRouter {
    @Value("${stargate.grpc.discovery.service}")
    private String grpcDiscovery;
    @Autowired
    private InfraService infraService;
    @Lazy
    @Autowired
    private PrometheusService prometheusService;

    @GetMapping(value = "readiness")
    public Mono<String> readiness(final ServerHttpRequest request) {
        return Mono.just("OK");
    }

    @GetMapping(value = "metrics", produces = CONTENT_TYPE_004)
    public Mono<String> metrics(final ServerHttpRequest request) {
        return prometheusService.metrics();
    }

    @GetMapping(value = "version")
    public Mono<VersionInfo> versionInfo(final ServerHttpRequest request) {
        return Mono.just(VERSION_INFO);
    }

    @PostMapping(value = "version")
    public Mono<VersionInfo> versionInfoViaPost(final ServerHttpRequest request) {
        return Mono.just(VERSION_INFO);
    }

    @GetMapping(value = "discovery/grpc")
    public Mono<String> grpcDiscovery(final ServerHttpRequest request) {
        return Mono.just(grpcDiscovery);
    }

    @GetMapping("favicon.ico")
    @ResponseBody
    void nofavicon() {
    }
}
