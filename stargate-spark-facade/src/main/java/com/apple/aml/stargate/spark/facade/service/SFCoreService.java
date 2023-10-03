package com.apple.aml.stargate.spark.facade.service;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
public class SFCoreService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public Mono<String> liveness() {
        return Mono.just("OK");
    }

    public Mono<String> readiness() {
        return Mono.just("OK");
    }

    public Flux<Map> fireSQL(final ServerHttpRequest request, final String sql) {
        Dataset<Row> rows = executeSql(sql);
        return Flux.fromStream(rows.collectAsList().parallelStream().map(row -> row.json())).map(s -> readJsonMap(s));
    }

    public Dataset<Row> executeSql( final String sql){
        Dataset<Row> rows = spark().sql(sql);
        return rows;
    }

    @SneakyThrows
    private SparkSession spark() {
        SparkSession sparkSession = null;
        try {
            sparkSession = SparkSession.active();
        } catch (Exception | Error e) {
            LOGGER.debug("Could not fetch any active Spark session;", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())));
            LOGGER.trace("Could not fetch any active Spark session;", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
        }
        if (sparkSession != null) return sparkSession;
        LOGGER.debug("Creating new spark session");
        sparkSession = SparkSession.builder().getOrCreate();
        LOGGER.debug("New spark session created successfully");
        return sparkSession;
    }
}
