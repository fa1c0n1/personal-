package com.apple.aml.stargate.app.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Service
public class QueryService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Mono<List<Map<String, Object>>> getEAIMetadataQueryResults(final long appId, final String query, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                sink.success(jdbcTemplate.queryForList(query));
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }
}
