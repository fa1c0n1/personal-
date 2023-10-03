package com.apple.examples.transformers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.function.Function;

public class Echo implements Function<Map<String, Object>, Map<String, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public Map<String, Object> apply(final Map<String, Object> input) {
        LOGGER.info("Echo apply method invoked with input - %s", input);
        return input;
    }
}
