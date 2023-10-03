package com.apple.aml.stargate.flink.source;

import com.apple.aml.stargate.beam.sdk.triggers.JsonLineRepeater;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.options.SequencerOptions;
import com.apple.aml.stargate.flink.functions.FreemarkerMapFunction;
import com.apple.aml.stargate.flink.functions.JavaMapFunction;
import com.apple.aml.stargate.flink.source.functions.SequenceSourceFunction;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import com.apple.aml.stargate.pipeline.sdk.trigger.BaseSequencer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.LOAD_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.jvm.commons.util.Strings.isBlank;


public final class SequencerSource implements FlinkSourceI {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public static void initRead(StargateNode node) {

    }

    @Override
    public EnrichedDataStream read(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
        SequencerOptions options = (SequencerOptions) node.getConfig();
        DataStream<Long> sequenceStream=null;
        if (options.getStartRate() >= 0 && options.getEndRate() > 0) {
            sequenceStream = enrichedDataStream.getExecutionEnvironment().addSource(new SequenceSourceFunction(0).withRate(1,options.getFrequency() == null ? Duration.ofMillis(1000) : Duration.ofMillis(options.getFrequency().toMillis()))).flatMap(new VariableRate(options.getStartRate(), options.getEndRate(), options.getStart()));
        } else {
            SequenceSourceFunction sequenceSourceFunction =null;
            sequenceSourceFunction =new SequenceSourceFunction(options.getStart());
            if (options.getStart() != options.getEnd()){
                sequenceSourceFunction = new SequenceSourceFunction(options.getStart(),options.getEnd());
            }
            sequenceSourceFunction = sequenceSourceFunction.withRate(1,options.getFrequency() == null ? Duration.ofMillis(1000) : Duration.ofMillis(options.getFrequency().toMillis()));
            sequenceStream=enrichedDataStream.getExecutionEnvironment().addSource(sequenceSourceFunction).setParallelism(1);
        }

        DataStream<Tuple2<String,GenericRecord>> sequenceSGStream = null;

        if (options.getLoadSize() >= 2) {
            sequenceSGStream =  sequenceStream.flatMap(new Multiply(node.name("multiply"), node.getType(), node.environment(), options.getLoadSize()));
        } else {
            sequenceSGStream =  sequenceStream.flatMap(new Operate(node.name("operate"), node.getType(), node.environment()));
        }

        if (!isBlank(options.getSchemaId()) || !isBlank(options.getSchemaIdExpression())) {
            if (isBlank(options.getFilePath())) {
                LOGGER.debug("schemaId/schemaIdExpression supplied. Will apply inbuilt freemarker transformer too..");
                sequenceSGStream= sequenceSGStream.process( new FreemarkerMapFunction(node));
            } else {
                LOGGER.debug("schemaId/schemaIdExpression supplied with filePath. Will apply filePath based generation");

                JavaFunctionOptions evaluatorOptions = new JavaFunctionOptions();
                evaluatorOptions.setContext(Map.of("filePath", options.getFilePath()));
                evaluatorOptions.setClassName(JsonLineRepeater.class.getCanonicalName());
                evaluatorOptions.setSchemaId(options.getSchemaId());
                evaluatorOptions.setSchemaReference(options.getSchemaReference());
                JavaMapFunction javaMapFunction = new JavaMapFunction(node,evaluatorOptions);
                sequenceSGStream = sequenceSGStream.process(javaMapFunction);
            }
        }
        return new EnrichedDataStream(enrichedDataStream.getExecutionEnvironment(),sequenceSGStream,null);

    }

    public static class Operate extends RichFlatMapFunction<Long, Tuple2<String, GenericRecord>> {
        private  ObjectToGenericRecordConverter converter;
        private final String nodeName;
        private final String nodeType;
        private final PipelineConstants.ENVIRONMENT environment;

        public Operate(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.environment=environment;

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.converter = converter(BaseSequencer.getSchema(this.environment));
        }

        @Override
        public void flatMap(Long seed, Collector<Tuple2<String, GenericRecord>> out) throws Exception {
            counter(nodeName, nodeType, UNKNOWN, "sequencer_invoked").inc();
            Instant instant = Instant.now();
            GenericRecord record = converter.convert(Map.of("value", seed, "epoc", instant.getMillis()));
            counter(nodeName, nodeType, record.getSchema().getFullName(), "elements_created").inc();
            out.collect(Tuple2.of(seed.toString(), record));
        }
    }


    public static class Multiply extends RichFlatMapFunction<Long, Tuple2<String, GenericRecord>> {
        private  ObjectToGenericRecordConverter converter;
        private final String nodeName;
        private final String nodeType;
        private final PipelineConstants.ENVIRONMENT environment;
        private final long loadSize;
        public Multiply(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment,final long loadSize) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.environment=environment;
            this.loadSize=loadSize;

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.converter = converter(BaseSequencer.getSchema(this.environment));
        }

        @Override
        public void flatMap(Long seed, Collector<Tuple2<String, GenericRecord>> out) throws Exception {
            counter(nodeName, nodeType, UNKNOWN, "sequencer_invoked").inc();
            for (int i = 0; i < loadSize; i++) {
                Instant instant = Instant.now();
                GenericRecord record = converter.convert(Map.of("value", seed, LOAD_NO, i, "epoc", instant.getMillis()));
                counter(nodeName, nodeType, record.getSchema().getFullName(), "elements_created").inc();
                out.collect(Tuple2.of(String.format("%s-%s", seed, i), record));
            }
        }
    }


    public static class VariableRate extends RichFlatMapFunction<Long, Long> {
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

        @Override
        public void flatMap(Long input, Collector<Long> out) throws Exception {
            long blockSize = base + rand.nextInt(vRate);
            for (long i = 0; i < blockSize; i++) out.collect(value.getAndIncrement());
        }
    }


}