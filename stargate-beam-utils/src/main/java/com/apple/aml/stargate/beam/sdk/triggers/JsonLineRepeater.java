package com.apple.aml.stargate.beam.sdk.triggers;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.SneakyThrows;
import org.apache.avro.generic.GenericRecord;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;

public class JsonLineRepeater implements Function<GenericRecord, Map<String, Object>> {
    private static final AtomicInteger ATOMIC = new AtomicInteger();
    private final List<Map<String, Object>> maps;

    @Inject
    @SuppressWarnings("unchecked")
    @SneakyThrows
    public JsonLineRepeater(@Named("context") final Map context) {
        maps = new ArrayList<>();
        for (String line : Files.readAllLines(Paths.get(context.get("filePath").toString()))) {
            maps.add((Map<String, Object>) ((Map) readJsonMap(line)));
        }
    }

    @Override
    public Map<String, Object> apply(final GenericRecord input) {
        return maps.get(Math.abs(ATOMIC.getAndIncrement() % maps.size()));
    }
}
