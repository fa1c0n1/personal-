package com.apple.aml.stargate.beam.sdk.triggers;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DataGeneratorOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.aml.stargate.common.constants.CommonConstants.FORMATTER_DATE_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.FORMATTER_DATE_TIME_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.LOAD_NO;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class DataGenerator {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {
        DataGeneratorOptions options = (DataGeneratorOptions) node.getConfig();
        PipelineConstants.ENVIRONMENT environment = node.environment();
        SAMPLER_TYPE samplerType = SAMPLER_TYPE.valueOf(options.getSamplerType() == null ? SAMPLER_TYPE.transaction.name() : options.getSamplerType().toLowerCase());
        if ("batch".equalsIgnoreCase(options.getGeneratorType())) {
            Compression compression = isBlank(options.getCompression()) ? Compression.UNCOMPRESSED : Compression.valueOf(options.getCompression().trim().toUpperCase());
            return SCollection.of(pipeline, pipeline.apply(Create.of(options.getFilePaths())).apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW)).apply(FileIO.readMatches().withCompression(compression)).apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())).via((SerializableFunction<FileIO.ReadableFile, KV<String, String>>) f -> {
                try {
                    return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
                } catch (Exception e) {
                    throw new GenericException("Error in reading file", Map.of("file", f.toString()), e).wrap();
                }
            })).apply(FlatMapElements.into(TypeDescriptors.strings()).via((KV<String, String> keyValuePair) -> Arrays.asList(keyValuePair.getValue().split("\n"))))).apply(node.name("freemarker-json"), new FreemarkerJsonDataDoFn<String>("line", options, environment, node.getName()));
        } else {
            SCollection<Long> window = SCollection.of(pipeline, pipeline.apply(node.getName(), GenerateSequence.from(options.getStart()).withRate(options.getRate(), options.getFrequency() == null ? Duration.standardSeconds(1) : Duration.millis(options.getFrequency().toMillis()))));
            if (samplerType == SAMPLER_TYPE.transaction) {
                return window.apply(node.name("txn"), new TransactionDataDoFn(options, environment));
            }
            return window.apply(node.name("freemarker"), new FreemarkerJsonDataDoFn<Long>("seed", options, environment, node.getName()));
        }
    }

    public enum SAMPLER_TYPE {
        transaction, freemarker
    }

    public static class FreemarkerJsonDataDoFn<O> extends DoFn<O, KV<String, GenericRecord>> {
        private static final AtomicLong atomicLong = new AtomicLong(0);
        private final String keyTemplateName;
        private final String payloadTemplateName;
        private final String seedName;
        private final DataGeneratorOptions options;
        private final Schema payloadSchema;
        private final ObjectToGenericRecordConverter payloadConverter;
        private transient Configuration configuration;

        public FreemarkerJsonDataDoFn(final String seedName, final DataGeneratorOptions options, final PipelineConstants.ENVIRONMENT environment, final String nodeName) {
            this.seedName = seedName;
            this.options = options;
            this.payloadSchema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
            this.payloadConverter = ObjectToGenericRecordConverter.converter(payloadSchema);
            this.keyTemplateName = nodeName + "~KEY";
            this.payloadTemplateName = nodeName + "~PAYLOAD";
        }

        @Setup
        public void setup() throws Exception {
            configuration();
        }

        @SuppressWarnings("unchecked")
        private Configuration configuration() {
            if (configuration != null) {
                return configuration;
            }
            loadFreemarkerTemplate(payloadTemplateName, this.options.getPayloadTemplate());
            if (this.options.getKeyTemplate() != null) {
                loadFreemarkerTemplate(keyTemplateName, this.options.getKeyTemplate());
            }
            configuration = freeMarkerConfiguration();
            return configuration;
        }

        @SuppressWarnings("deprecation")
        @ProcessElement
        public void processElement(@Element final O seed, final ProcessContext ctx) throws Exception {
            Configuration configuration = configuration();
            String keyPrefix = null;
            if (this.options.getKeyTemplate() == null) {
                keyPrefix = FORMATTER_DATE_TIME_1.format(Instant.now()) + "-" + UUID.randomUUID();
            }
            for (int i = 0; i < options.getLoadSize(); i++) {
                long counter = atomicLong.getAndIncrement();
                String key;
                if (keyPrefix != null) {
                    key = keyPrefix + "-" + i;
                } else {
                    key = evaluateFreemarker(configuration, keyTemplateName, null, null, null, this.seedName, seed, LOAD_NO, counter).trim();
                }
                String payload = evaluateFreemarker(configuration, payloadTemplateName, key, null, null, this.seedName, seed, LOAD_NO, counter);
                GenericRecord record = payloadConverter.convert(payload);
                ctx.output(KV.of(key, record));
            }
        }
    }

    public static class TransactionDataDoFn extends DoFn<Long, KV<String, GenericRecord>> {
        private final DataGeneratorOptions options;
        private final boolean markerNeeded;
        private final PipelineConstants.ENVIRONMENT environment;
        private final Schema markerSchema;
        private final ObjectToGenericRecordConverter markerConverter;
        private final Schema payloadSchema;
        private final ObjectToGenericRecordConverter payloadConverter;

        public TransactionDataDoFn(final DataGeneratorOptions options, final PipelineConstants.ENVIRONMENT environment) {
            this.options = options;
            this.environment = environment;
            this.markerNeeded = options.getMarkerSchemaId() != null && options.getMarkerTemplate() != null;
            this.markerSchema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getMarkerSchemaId());
            this.markerConverter = markerNeeded ? ObjectToGenericRecordConverter.converter(markerSchema) : null;
            this.payloadSchema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
            this.payloadConverter = ObjectToGenericRecordConverter.converter(payloadSchema);
        }

        @ProcessElement
        public void processElement(@Element final Long seed, final ProcessContext ctx) throws Exception {
            Instant instant = Instant.now();
            String transactionId = FORMATTER_DATE_TIME_1.format(instant) + "-" + UUID.randomUUID();
            String markerId = UUID.randomUUID().toString();
            String markerEpoc = String.valueOf(instant.toEpochMilli());
            String dateString = FORMATTER_DATE_1.format(instant);
            String dateTimeString = FORMATTER_DATE_TIME_1.format(instant);

            String markerTemplate = null;
            if (options.getMarkerSchemaId() != null && options.getMarkerTemplate() != null) {
                markerId = transactionId;
                markerTemplate = options.getMarkerTemplate();
                markerTemplate = markerTemplate.replace("TOKEN_MARKER_UUID", markerId);
                markerTemplate = markerTemplate.replace("TOKEN_MARKER_EPOC", markerEpoc);
                markerTemplate = markerTemplate.replace("TOKEN_MARKER_DATE", dateString);
                markerTemplate = markerTemplate.replace("TOKEN_MARKER_DATETIME", dateTimeString);
                String startMarker = markerTemplate.replace("TOKEN_MARKER_STATUS", "START");
                GenericRecord marker = markerConverter.convert(startMarker);
                for (int i = 0; i < options.getPartitions(); i++) {
                    ctx.outputWithTimestamp(KV.of(transactionId + "-START-" + i, marker), org.joda.time.Instant.now());
                }
            }
            Random random = new Random(System.nanoTime() + seed);
            for (int i = 0; i < options.getLoadSize(); i++) {
                String payload = options.getPayloadTemplate();
                Instant now = Instant.now();
                payload = payload.replace("TOKEN_UUID", UUID.randomUUID().toString());
                payload = payload.replace("TOKEN_INT", String.valueOf(random.nextInt()));
                payload = payload.replace("TOKEN_LONG", String.valueOf(random.nextLong()));
                payload = payload.replace("TOKEN_DOUBLE", String.valueOf(random.nextDouble()));
                payload = payload.replace("TOKEN_BOOLEAN", String.valueOf(random.nextBoolean()));
                payload = payload.replace("TOKEN_STRING", "string-" + random.nextInt());
                payload = payload.replace("TOKEN_EPOC", String.valueOf(now.toEpochMilli()));
                payload = payload.replace("TOKEN_DATETIME", FORMATTER_DATE_TIME_1.format(now));
                payload = payload.replace("TOKEN_MARKER_UUID", markerId);
                payload = payload.replace("TOKEN_MARKER_EPOC", markerEpoc);
                payload = payload.replace("TOKEN_MARKER_DATE", dateString);
                payload = payload.replace("TOKEN_MARKER_DATETIME", dateTimeString);
                GenericRecord record = payloadConverter.convert(payload);
                int partition = random.nextInt(options.getPartitions());
                ctx.outputWithTimestamp(KV.of(transactionId + "-" + partition + "-" + UUID.randomUUID(), record), org.joda.time.Instant.now());
            }
            if (markerTemplate != null) {
                String startMarker = markerTemplate.replace("TOKEN_MARKER_STATUS", "END");
                GenericRecord marker = markerConverter.convert(startMarker);
                for (int i = 0; i < options.getPartitions(); i++) {
                    ctx.outputWithTimestamp(KV.of(transactionId + "-END-" + i, marker), org.joda.time.Instant.now());
                }
            }
        }
    }
}
