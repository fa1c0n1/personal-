package com.apple.aml.stargate.common.spring.service;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.common.TextFormat;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.jvmTimeMetric;
import static com.apple.aml.stargate.common.utils.TemplateUtils.yyyyMMddHHmmss;

@Service
public class PrometheusService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    @Lazy
    private PrometheusMeterRegistry meterRegistry;
    private Gauge jvmTimeMetric;

    @PostConstruct
    void init() {
        jvmTimeMetric = jvmTimeMetric();
    }

    public Mono<String> metrics() {
        return Mono.create(sink -> {
            try {
                Instant now = Instant.now();
                long milli = now.toEpochMilli();
                jvmTimeMetric.labels("epoc").set(milli);
                jvmTimeMetric.labels("epoc_formatted").set(Long.parseLong(yyyyMMddHHmmss(milli)));
                StringWriter writer = new StringWriter();
                TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
                TextFormat.write004(writer, meterRegistry.getPrometheusRegistry().metricFamilySamples());
                writer.close();
                sink.success(writer.toString());
            } catch (Exception e) {
                LOGGER.warn("Could not collect spring metric buffer", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }
}
