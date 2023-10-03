package com.apple.examples.transformers;

import com.apple.aml.stargate.common.services.DhariService;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class DhariSample implements Function<Map<String, Object>, Map<String, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    @Inject
    @Named("context")
    private Map<String, Object> context;
    @Inject
    private DhariService dhariService;

    @Override
    @SneakyThrows
    public Map<String, Object> apply(final Map<String, Object> input) {
        Map<String, ?> dhariResponse = dhariService.ingest((String) context.get("payloadPublisherId"), (String) context.get("payloadSchemaId"), UUID.randomUUID().toString(), input);
        LOGGER.info("Dhari response - %s", dhariResponse);
        return input;
    }
}