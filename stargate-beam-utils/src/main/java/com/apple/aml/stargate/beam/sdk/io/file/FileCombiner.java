package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.common.constants.PipelineConstants.ENVIRONMENT;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;

public class FileCombiner<O> extends Combine.CombineFn<KV<String, O>, Map<String, Set<O>>, List<KV<String, GenericRecord>>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String schemaId;
    private final Schema schema;
    private final ObjectToGenericRecordConverter converter;

    public FileCombiner(final ENVIRONMENT environment, final String schemaId, final String schemaReference) {
        if (schemaId == null || schemaId.isBlank()) {
            this.schema = getSchema(environment);
            this.schemaId = this.schema.getFullName();
            this.converter = ObjectToGenericRecordConverter.converter(schema);
        } else {
            this.schemaId = schemaId;
            this.schema = fetchSchemaWithLocalFallback(schemaReference, this.schemaId);
            this.converter = converter(this.schema);
        }
        LOGGER.debug("FileCombiner schema converter created successfully for", Map.of(SCHEMA_ID, this.schemaId));
    }

    private static Schema getSchema(ENVIRONMENT environment) {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = getSchemaString().replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    private static String getSchemaString() {
        try {
            String schemaString = IOUtils.resourceToString("/filecombiner.avsc", Charset.defaultCharset());
            if (schemaString == null || schemaString.isBlank()) {
                throw new Exception("Could not load schemaString");
            }
            return schemaString;
        } catch (Exception e) {
            return "{\"type\": \"record\",\"name\": \"FileCombiner\",\"namespace\": \"com.apple.aml.stargate.#{ENV}.internal\",\"fields\": [{\"name\": \"key\",\"type\": \"string\"},{\"name\": \"values\",\"type\": [\"null\",{\"type\": \"array\",\"items\": \"string\"}]}]}";
        }
    }

    @Override
    public Map<String, Set<O>> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Set<O>> addInput(final Map<String, Set<O>> accumulator, final KV<String, O> input) {
        accumulator.computeIfAbsent(input.getKey(), k -> new HashSet<>()).add(input.getValue());
        return accumulator;
    }

    @Override
    public Map<String, Set<O>> mergeAccumulators(Iterable<Map<String, Set<O>>> accumulators) {
        Iterator<Map<String, Set<O>>> itr = accumulators.iterator();
        if (itr.hasNext()) {
            Map<String, Set<O>> first = itr.next();
            while (itr.hasNext()) {
                for (Map.Entry<String, Set<O>> entry : itr.next().entrySet()) {
                    first.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).addAll(entry.getValue());
                }
            }
            return first;
        } else {
            return new HashMap<>();
        }
    }

    @Override
    public List<KV<String, GenericRecord>> extractOutput(Map<String, Set<O>> map) {
        final List<KV<String, GenericRecord>> list = new ArrayList<>();
        map.entrySet().forEach(entry -> {
            try {
                Set<O> values = entry.getValue();
                if (values.isEmpty()) {
                    return;
                }
                List<Object> vmaps = values.stream().map(x -> {
                    try {
                        return ((x instanceof IndexedRecord) || x instanceof GenericData) ? readJsonMap(x.toString()) : x;
                    } catch (Exception e) {
                        return x;
                    }
                }).collect(Collectors.toList());
                KV<String, GenericRecord> kv = KV.of(entry.getKey(), converter.convert(Map.of("key", entry.getKey(), "values", vmaps)));
                list.add(kv);
            } catch (Exception e) {
                throw new GenericException("Error while combining records", Map.of("key", entry.getKey(), "value", entry.getValue()), e).wrap();
            }
        });
        return list;
    }
}
