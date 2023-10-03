package com.apple.aml.stargate.beam.sdk.utils;

import com.apple.aml.stargate.beam.sdk.io.file.GenericRecordTextSink;
import com.apple.aml.stargate.beam.sdk.io.file.InMemoryWritableByteChannel;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.FileOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static java.util.Arrays.asList;

public final class FileWriterFns {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    private static final Instant JVM_START_TIME = new Instant(ManagementFactory.getRuntimeMXBean().getStartTime());
    private static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder().appendHours().appendSuffix("h").appendMinutes().appendSuffix("m").appendSeconds().appendSuffix("s").toFormatter();


    private FileWriterFns() {

    }

    public static String compressionCodec(final FileOptions options) {
        CompressionCodecName codec = codecName(options);
        return codec == null ? options.getFileCompression() : codec.name().toLowerCase();
    }

    public static CompressionCodecName codecName(final FileOptions options) {
        try {
            return options.getFileCompression() == null || options.getFileCompression().isBlank() ? CompressionCodecName.SNAPPY : CompressionCodecName.valueOf(options.getFileCompression().trim().toUpperCase());
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static synchronized WriterValue createFileWriter(final WriterKey key, final Schema schema, final String extension, final String basePath, final PipelineConstants.DATA_FORMAT fileFormat, final CompressionCodecName codecName, final boolean useBulkWriter, final SchemaLevelOptions options) throws IOException {
        String windowSuffix;
        String timestampSuffix;
        long start = Long.parseLong(DATE_TIME_FORMAT.print(key.start));
        windowSuffix = DATE_TIME_FORMAT.print(key.end) + "-" + start;
        timestampSuffix = PERIOD_FORMATTER.print(new Duration(JVM_START_TIME.isBefore(key.start) ? key.start : JVM_START_TIME, key.timestamp).toPeriod());
        long paneIndex = key.paneIndex;
        final String filePattern = String.format("%s/%s%s/%s-%s-%s-%s", basePath, key.filePath, key.partitionPath, (paneIndex < 0 ? "x" : paneIndex), windowSuffix, timestampSuffix, key.workerId);
        LOGGER.debug("Creating a new writer for current writerKey", Map.of(SCHEMA_ID, key.schemaId, "version", key.version, "paneIndex", key.paneIndex, "windowStart", key.start, "windowEnd", key.end, "workerId", key.workerId, "filePath", key.filePath, "partitionPath", key.partitionPath));
        List<MatchResult> results = FileSystems.match(asList(filePattern + "-*"));
        int fileIndex = (results == null || results.isEmpty()) ? 0 : (results.stream().filter(x -> x.status() == MatchResult.Status.OK).mapToInt(x -> {
            try {
                return x.metadata().size();
            } catch (Exception e) {
                return 0;
            }
        }).max().orElse(0) + 1);
        String fileName = String.format("%s-%04d-%s.%s", filePattern, fileIndex, Thread.currentThread().getId(), extension);
        WritableByteChannel channel;
        if (useBulkWriter) {
            channel = new InMemoryWritableByteChannel(key, fileName);
        } else {
            ResourceId resourceId = FileSystems.matchNewResource(fileName, false);
            channel = FileSystems.create(resourceId, MimeTypes.BINARY);
        }
        FileIO.Sink sink;
        switch (fileFormat) {
            case csv:
                if (options.isIncludeXSVHeader()) {
                    sink = new GenericRecordTextSink(schema.getFields().stream().map(f -> f.name()).collect(Collectors.joining(options.getXsvDelimiter())), record -> schema.getFields().stream().map(f -> String.valueOf(record.get(f.name()))).collect(Collectors.joining(options.getXsvDelimiter())));
                } else {
                    sink = new GenericRecordTextSink(record -> schema.getFields().stream().map(f -> String.valueOf(record.get(f.name()))).collect(Collectors.joining(options.getXsvDelimiter())));
                }
                break;
            case avro:
                sink = AvroIO.sink(schema);
                break;
            case parquet:
                sink = ParquetIO.sink(schema).withCompressionCodec(codecName);//.withConfiguration(hadoopConfiguration()); // TODO : need to check if we really need to pass `.withConfiguration(hadoopConfiguration())` ? ( works fine for S3 & File; not sure of hdfs )
                break;
            case json:
                sink = new GenericRecordTextSink(Object::toString);
                break;
            default:
                sink = TextIO.sink();
                break;
        }
        sink.open(channel);
        key.setCreatedOn(Instant.now());
        final WriterValue value = new FileWriterValue(key, sink, channel, fileName);
        LOGGER.debug("File writer created successfully for", Map.of(SCHEMA_ID, key.schemaId, "paneIndex", paneIndex, "windowStart", key.start, "windowEnd", key.end, "workerId", key.workerId, "filePath", key.filePath, "partitionPath", key.partitionPath, "fileName", fileName));
        return value;
    }

    @Getter
    @Setter
    public static class WriterKey implements Serializable {
        private final String nodeName;
        private final String nodeType;
        private final String schemaId;
        private final int version;
        private final Instant start;
        private final Instant end;
        private final long paneIndex;
        private final String workerId;
        private final String filePath;
        private final String partitionPath;
        private final String uniqueKey;
        private final Instant timestamp;
        private Instant createdOn;

        public WriterKey(final String nodeName, final String nodeType, final String schemaId, final int version, final BoundedWindow window, final PaneInfo paneInfo, final String filePath, final String partitionPath, final Instant timestamp) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.workerId = PipelineUtils.workerId();
            this.schemaId = schemaId;
            this.version = version;
            this.paneIndex = paneInfo == null ? -1 : paneInfo.getIndex();
            this.filePath = filePath;
            this.partitionPath = partitionPath;
            this.timestamp = timestamp;
            if (window instanceof IntervalWindow) {
                IntervalWindow intervalWindow = (IntervalWindow) window;
                this.start = intervalWindow.start();
                this.end = intervalWindow.end();
            } else {
                this.start = window.maxTimestamp();
                this.end = this.start;
            }
            this.uniqueKey = String.format("%s^%s^%d^%s^%s^%s^%d", workerId, schemaId, version, filePath, partitionPath, window, paneInfo == null ? -1 : paneInfo.getIndex());
        }

        @Override
        public int hashCode() {
            return uniqueKey.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof WriterKey)) {
                return false;
            }
            WriterKey other = (WriterKey) o;
            return uniqueKey.equals(other.uniqueKey);
        }

        public Map<String, Object> logMap() {
            return Map.of("writerWorkerId", workerId, "writerSchemaId", schemaId, "writerFilePath", filePath, "writerPartitionPath", partitionPath, "writerWindow", String.format("%s-%s", start, end), "writerPaneIndex", paneIndex, NODE_NAME, nodeName, NODE_TYPE, nodeType);
        }
    }

    public static abstract class WriterValue {
        @Getter
        private final WriterKey writerKey;
        @Getter
        private final String fileName;
        @Getter
        private List<KV<String, GenericRecord>> successRecords;

        public WriterValue(final WriterKey writerKey, final String fileName) {
            this.writerKey = writerKey;
            this.fileName = fileName;
            this.successRecords = new ArrayList<>();
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[]{fileName});
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof WriterValue)) {
                return false;
            }
            WriterValue other = (WriterValue) o;
            return Objects.equals(fileName, other.fileName);
        }

        public synchronized void write(final KV<String, GenericRecord> kv) throws Exception {
            writeRecord(kv.getValue());
            successRecords.add(kv);
        }

        public abstract void writeRecord(final GenericRecord record) throws Exception;

        public int count() {
            return successRecords.size();
        }

        public abstract void close() throws IOException;

        public abstract void abort() throws IOException;
    }

    public static class FileWriterValue extends WriterValue {

        private final FileIO.Sink<GenericRecord> writer;
        private WritableByteChannel channel;

        public FileWriterValue(final WriterKey writerKey, final FileIO.Sink<GenericRecord> writer, final WritableByteChannel channel, final String fileName) {
            super(writerKey, fileName);
            this.writer = writer;
            this.channel = channel;
        }

        @Override
        public void writeRecord(final GenericRecord record) throws Exception {
            writer.write(record);
        }

        @Override
        public void close() throws IOException {
            if (channel == null) {
                return;
            }
            try {
                writer.flush();
            } finally {
                if (channel.isOpen()) {
                    channel.close();
                }
                channel = null;
            }
        }

        @Override
        public void abort() throws IOException {
            close();
        }
    }
}
