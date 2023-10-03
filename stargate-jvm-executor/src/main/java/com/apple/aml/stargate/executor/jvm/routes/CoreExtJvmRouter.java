package com.apple.aml.stargate.executor.jvm.routes;

import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/")
public class CoreExtJvmRouter {
    @GetMapping(value = "readiness")
    public Mono<String> readiness(final ServerHttpRequest request) {
        return Mono.just("OK");
    }

    @PostMapping(value = "init/lambda")
    public Mono<Map<String, Object>> initLambda(@RequestBody Map<String, Object> map, final ServerHttpRequest request) {
        return Mono.just(map);
    }

    @PostMapping(value = "execute/lambda")
    public Mono<Map<String, Object>> executeLambda(@RequestBody Map<String, Object> map, final ServerHttpRequest request) {
        return Mono.just(map);
    }

    @PostMapping(value = "init/template")
    public Mono<Map<String, Object>> initTemplate(@RequestBody Map<String, Object> map, final ServerHttpRequest request) {
        return Mono.just(map);
    }

    @PostMapping(value = "execute/template")
    public Mono<Map<String, Object>> executeTemplate(@RequestBody Map<String, Object> map, final ServerHttpRequest request) {
        return Mono.just(map);
    }

}
