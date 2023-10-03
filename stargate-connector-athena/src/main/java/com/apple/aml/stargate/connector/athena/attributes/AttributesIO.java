package com.apple.aml.stargate.connector.athena.attributes;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.AttributesOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class AttributesIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public static Schema getGenericAttributesSchema(final PipelineConstants.ENVIRONMENT environment) {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = getGenericAttributesSchemaString().replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    public static String getGenericAttributesSchemaString() {
        try {
            String schemaString = IOUtils.resourceToString("/stringvsany.avsc", Charset.defaultCharset());
            if (schemaString == null || schemaString.isBlank()) {
                throw new Exception("Could not load schemaString");
            }
            return schemaString;
        } catch (Exception e) {
            return "{\"type\": \"record\",\"name\": \"StringVsAny\",\"namespace\": \"com.apple.aml.stargate.#{ENV}.internal\",\"fields\": [{\"name\": \"key\",\"type\": \"string\"},{\"name\": \"value\",\"type\": [\"null\",{\"type\": \"map\", \"values\": \"string\"},\"string\"]}]}";
        }
    }

    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {
        AttributesOptions options = (AttributesOptions) node.getConfig();
        Collection<String> keys = options.getKeys();
        if (options.getLookupName() != null) {
            if (options.getLookupKeys() != null) {
                final Set<String> allkeys = new HashSet<String>();
                if (keys != null) {
                    allkeys.addAll(keys);
                }
                options.getLookupKeys().stream().map(k -> allkeys.add(options.getLookupName() + ":" + k));
                keys = allkeys;
            }
        }
        if (keys == null || keys.isEmpty()) {
            throw new InvalidInputException("Empty keys supplied in attributes reader node!!");
        }
        AttributesOptions cloned = duplicate(options, AttributesOptions.class);
        cloned.setLookupKeys(null);
        cloned.setLookupName(null);
        cloned.setLookupKeysAttribute(null);
        cloned.setLookupNameAttribute(null);
        cloned.setKeys(null);
        cloned.setKeysAttribute(null);
        cloned.setKeyAttribute("key");
        return SCollection.apply(pipeline, node.name("key-reader"), Create.of(keys)).apply(node.name("avro-converter"), new AvroKey(node.environment())).apply(node.getName(), new AttributesReader(node, cloned));
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        AttributesOptions options = (AttributesOptions) node.getConfig();
        if (options.getOperation() == null || "read".equalsIgnoreCase(options.getOperation())) {
            return collection.apply(node.getName(), new AttributesReader(node, options));
        }
        return collection.apply(node.getName(), new AttributesWriter(node, options, true));
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), new AttributesWriter(node, (AttributesOptions) node.getConfig(), false));
    }

    public static class AvroKey extends DoFn<String, KV<String, GenericRecord>> {
        private static final long serialVersionUID = 1L;
        private final ObjectToGenericRecordConverter converter;

        public AvroKey(final PipelineConstants.ENVIRONMENT environment) {
            converter = ObjectToGenericRecordConverter.converter(getGenericAttributesSchema(environment));
        }

        @ProcessElement
        public void processElement(final @Element String key, final ProcessContext ctx) throws Exception {
            ctx.outputWithTimestamp(KV.of(key, converter.convert("{\"key\":\"" + key + "\"}")), Instant.now());
        }
    }
}
