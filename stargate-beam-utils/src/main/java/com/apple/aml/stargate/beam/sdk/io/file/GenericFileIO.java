package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.beam.sdk.transforms.CollectionFns;
import com.apple.aml.stargate.beam.sdk.utils.WindowFns;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FileOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.transforms.UtilFns.recordKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchPartitionKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_GROUP_TAG;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static org.apache.beam.sdk.io.Compression.UNCOMPRESSED;

public class GenericFileIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {
        FileOptions options = (FileOptions) node.getConfig();
        Schema schema = isBlank(options.getSchemaId()) ? null : fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
        Compression compression = isBlank(options.getFileCompression()) ? UNCOMPRESSED : Compression.valueOf(options.getFileCompression().trim().toUpperCase());
        String filePattern = options.basePath() + (isBlank(options.getFilePath()) ? (isBlank(options.getFilePattern()) ? String.format("/*.%s", options.fileFormat().name()) : options.getFilePattern()) : options.getFilePath());
        PCollection<MatchResult.Metadata> fileMetadata = pipeline.apply(FileIO.match().filepattern(filePattern));
        PCollection<FileIO.ReadableFile> files = compression == UNCOMPRESSED ? fileMetadata.apply(node.name("read-files"), FileIO.readMatches()) : fileMetadata.apply(node.name("read-files-with-compression"), FileIO.readMatches().withCompression(compression));
        ObjectToGenericRecordConverter converter = schema == null ? null : converter(schema, options);
        String recordIdentifier = isBlank(options.getRecordIdentifier()) ? null : options.getRecordIdentifier().trim();
        Coder<GenericRecord> avroCoder = pipeline.getCoderRegistry().getCoder(GenericRecord.class);
        Coder<KV<String, GenericRecord>> kvCoder = KvCoder.of(StringUtf8Coder.of(), avroCoder);
        PCollection<KV<String, GenericRecord>> collection = files.apply(node.name("read-parquet"), ParquetIO.parseFilesGenericRecords((SerializableFunction<GenericRecord, KV<String, GenericRecord>>) input -> {
            if (input == null) return null;
            if (converter == null) return KV.of(recordKey(input, recordIdentifier), input);
            try {
                GenericRecord record = converter.convert(input.toString());
                return KV.of(recordKey(record, recordIdentifier), record);
            } catch (Exception e) {
                String key = recordKey(input, recordIdentifier);
                LOGGER.warn("Could not convert the input record to required schema. Will return primitive record as-is", Map.of("inputSchemaId", input.getSchema().getFullName(), "key", key), e);
                return KV.of(key, input);
            }
        }).withCoder(kvCoder));
        return SCollection.of(pipeline, collection);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return transformCollection(pipeline, node, inputCollection, false);
    }

    protected SCollection<KV<String, GenericRecord>> transformCollection(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection, final boolean emit) throws Exception {
        FileOptions options = (FileOptions) node.getConfig();
        PipelineConstants.ENVIRONMENT environment = node.environment();
        SCollection<KV<String, GenericRecord>> window = applyWindow(inputCollection, options, node.getName());
        if (options.getBatchSize() > 1 || !isBlank(options.getBatchBytes()) || (options.getBatchDuration() != null && options.getBatchDuration().getSeconds() > 0)) {
            LOGGER.debug("Optimized batching enabled. Will apply batching", Map.of("batchSize", options.getBatchSize(), "batchBytes", String.valueOf(options.getBatchBytes()), "batchDuration", options.batchDuration(), "partitions", options.getPartitions()));
            boolean isNoOp = node.getType().toLowerCase().startsWith("noop");
            window = window.group(node.name("partition-key"), batchPartitionKey(options.getPartitions(), options.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("batch"), batchWriter(node.getName(), node.getType(), environment, options, isNoOp ? new NoOpFileWriter<>(node.getName(), node.getType()) : new BaseFileWriter<>(node.getName(), node.getType(), environment, options, emit)));
        } else if (options.isShuffle()) {
            LOGGER.debug("Shuffle enabled. Will apply shuffle");
            window = window.list(node.name("gather-window"), new WindowFns.GatherResults<>()).apply(node.name("write"), new FileWriter<>(node.getName(), node.getType(), environment, options));
        } else {
            window = window.apply(node.name("write"), new FileWriter<>(node.getName(), node.getType(), environment, options));
        }
        if (!options.isCombine()) {
            return window;
        }
        return window.list(node.name("combine"), Combine.globally(new FileCombiner<GenericRecord>(environment, options.getCombinerSchemaId(), options.getSchemaReference())).withoutDefaults()).apply(node.name("result"), new CollectionFns.IndividualRecords<>());
    }

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        LOGGER.debug("Setting up generic file io writer configs - started");
        FileOptions options = (FileOptions) node.getConfig();
        options.enableSchemaLevelDefaults();
        LOGGER.debug("Setting up generic file io writer configs - successful");
    }

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        LOGGER.debug("Setting up generic file io transformer configs - started");
        FileOptions options = (FileOptions) node.getConfig();
        options.enableSchemaLevelDefaults();
        LOGGER.debug("Setting up generic file io transformer configs - successful");
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return transformCollection(pipeline, node, collection, true);
    }
}
