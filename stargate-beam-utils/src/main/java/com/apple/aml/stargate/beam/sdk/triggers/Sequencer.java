package com.apple.aml.stargate.beam.sdk.triggers;

import com.apple.aml.stargate.beam.sdk.ts.FreemarkerEvaluator;
import com.apple.aml.stargate.beam.sdk.ts.JavaFunction;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.options.SequencerOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.LOAD_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static org.joda.time.Duration.standardSeconds;

public final class Sequencer {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {
        SequencerOptions options = (SequencerOptions) node.getConfig();
        SCollection<Long> collection;
        if (options.getStartRate() >= 0 && options.getEndRate() > 0) {
            GenerateSequence sequence = GenerateSequence.from(0);
            collection = SCollection.of(pipeline, pipeline.apply(node.name("generate-base"), sequence.withRate(1, options.getFrequency() == null ? standardSeconds(1) : Duration.millis(options.getFrequency().toMillis()))).apply("variable-rate", ParDo.of(new VariableRate(options.getStartRate(), options.getEndRate(), options.getStart()))));
        } else {
            GenerateSequence sequence = GenerateSequence.from(options.getStart());
            if (options.getStart() != options.getEnd()) sequence = sequence.to(options.getEnd());
            collection = SCollection.of(pipeline, pipeline.apply(node.name("generate"), sequence.withRate(options.getRate(), options.getFrequency() == null ? standardSeconds(1) : Duration.millis(options.getFrequency().toMillis()))));
        }
        SCollection<KV<String, GenericRecord>> stream;
        if (options.getLoadSize() >= 2) {
            stream = collection.apply(node.name("multiply"), new Multiply(node.name("multiply"), node.getType(), getSchema(node.environment()), options.getLoadSize()));
        } else {
            stream = collection.apply(node.name("operate"), new Operate(node.name("operate"), node.getType(), getSchema(node.environment())));
        }
        if (!isBlank(options.getSchemaId()) || !isBlank(options.getSchemaIdExpression())) {
            if (isBlank(options.getFilePath())) {
                LOGGER.debug("schemaId/schemaIdExpression supplied. Will apply inbuilt freemarker transformer too..");
                FreemarkerEvaluator evaluator = new FreemarkerEvaluator();
                evaluator.initFreemarkerNode(pipeline, node, true);
                stream = stream.apply(node.name("data"), evaluator);
            } else {
                LOGGER.debug("schemaId/schemaIdExpression supplied with filePath. Will apply filePath based generation");
                JavaFunction evaluator = new JavaFunction();
                JavaFunctionOptions evaluatorOptions = new JavaFunctionOptions();
                evaluatorOptions.setContext(Map.of("filePath", options.getFilePath()));
                evaluatorOptions.setClassName(JsonLineRepeater.class.getCanonicalName());
                evaluatorOptions.setSchemaId(options.getSchemaId());
                evaluatorOptions.setSchemaReference(options.getSchemaReference());
                evaluator.initJavaFunctionNode(pipeline, node, evaluatorOptions);
                stream = stream.apply(node.name("data"), evaluator);
            }
        }
        return stream;
    }

    public static Schema getSchema(PipelineConstants.ENVIRONMENT environment) {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = getSchemaString().replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    public static String getSchemaString() {
        try {
            String schemaString = IOUtils.resourceToString("/sequencer.avsc", Charset.defaultCharset());
            if (schemaString == null || schemaString.isBlank()) {
                throw new Exception("Could not load schemaString");
            }
            return schemaString;
        } catch (Exception e) {
            return "{\"type\":\"record\",\"name\":\"Sequencer\",\"namespace\":\"com.apple.aml.stargate.#{ENV}.internal\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"epoc\",\"type\":\"long\"},{\"name\":\"loadNo\",\"type\":[\"null\",\"long\"]}]}";
        }
    }

    public static class Operate extends DoFn<Long, KV<String, GenericRecord>> {
        private final ObjectToGenericRecordConverter converter;
        private final String nodeName;
        private final String nodeType;

        public Operate(final String nodeName, final String nodeType, final Schema schema) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.converter = converter(schema);
        }

        @ProcessElement
        public void processElement(@Element final Long seed, final ProcessContext ctx) throws Exception {
            counter(nodeName, nodeType, UNKNOWN, "sequencer_invoked").inc();
            Instant instant = Instant.now();
            GenericRecord record = converter.convert(Map.of("value", seed, "epoc", instant.getMillis()));
            counter(nodeName, nodeType, record.getSchema().getFullName(), "elements_created").inc();
            ctx.outputWithTimestamp(KV.of(seed.toString(), record), instant);
        }
    }

    public static class Multiply extends DoFn<Long, KV<String, GenericRecord>> {
        private final long loadSize;
        private final ObjectToGenericRecordConverter converter;
        private final String nodeName;
        private final String nodeType;

        public Multiply(final String nodeName, final String nodeType, final Schema schema, final long loadSize) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.converter = converter(schema);
            this.loadSize = loadSize;
        }

        @ProcessElement
        public void processElement(@Element final Long seed, final ProcessContext ctx) throws Exception {
            counter(nodeName, nodeType, UNKNOWN, "sequencer_invoked").inc();
            for (int i = 0; i < loadSize; i++) {
                Instant instant = Instant.now();
                GenericRecord record = converter.convert(Map.of("value", seed, LOAD_NO, i, "epoc", instant.getMillis()));
                counter(nodeName, nodeType, record.getSchema().getFullName(), "elements_created").inc();
                ctx.outputWithTimestamp(KV.of(String.format("%s-%s", seed, i), record), instant);
            }
        }
    }

    public static class VariableRate extends DoFn<Long, Long> {
        private final long base;
        private final int vRate;
        private final AtomicLong value;
        private final Random rand;

        public VariableRate(final long startRate, final long endRate, final long start) {
            this.base = startRate;
            this.vRate = (int) (endRate - startRate);
            this.value = new AtomicLong(start);
            this.rand = new Random(System.currentTimeMillis());
        }

        @ProcessElement
        public void processElement(final ProcessContext ctx) throws Exception {
            long blockSize = base + rand.nextInt(vRate);
            for (long i = 0; i < blockSize; i++) ctx.output(value.getAndIncrement());
        }
    }
}
