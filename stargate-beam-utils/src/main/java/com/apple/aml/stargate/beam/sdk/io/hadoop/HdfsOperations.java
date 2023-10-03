package com.apple.aml.stargate.beam.sdk.io.hadoop;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.HdfsOpsOptions;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.FormatUtils.evaluateSpel;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.hadoopConfiguration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.isDynamicKerberosLoginEnabled;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performKerberosLogin;
import static java.util.Arrays.asList;

public class HdfsOperations extends HdfsIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    public static void init(final PipelineOptions pipelineOptions, final StargateNode node) throws Exception {
        LOGGER.debug("Setting up HdfsOperations configs - started");
        HdfsIO.init(pipelineOptions, node);
        LOGGER.debug("Setting up HdfsOperations configs - successful");
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        HdfsOpsOptions options = (HdfsOpsOptions) node.getConfig();
        OPERATION_TYPE operationType = OPERATION_TYPE.valueOf(options.getOperation().toLowerCase());
        SCollection<KV<String, GenericRecord>> window = collection;
        if (operationType == OPERATION_TYPE.move) {
            return window.apply(node.name("move"), new CopyOperation(node.getName(), node.getType(), options.logPayloadLevel(), true, options.getSource(), options.getDestination(), options.basePath(), options.isSequence()));
        }
        if (operationType == OPERATION_TYPE.copy) {
            return window.apply(node.name("copy"), new CopyOperation(node.getName(), node.getType(), options.logPayloadLevel(), false, options.getSource(), options.getDestination(), options.basePath(), options.isSequence()));
        }
        if (operationType == OPERATION_TYPE.delete) {
            return window.apply(node.name("delete"), new DeleteOperation(node.getName(), node.getType(), options.logPayloadLevel(), options.getPaths(), options.basePath()));
        }
        if (operationType == OPERATION_TYPE.noop) {
            return window;
        }
        return super.transformCollection(pipeline, node, window, true);
    }

    private enum OPERATION_TYPE {
        copy, move, delete, noop
    }

    public static class CopyOperation extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
        private final String globalSource;
        private final String globalDestination;
        private final String basePath;
        private final boolean deleteSourceFile;
        private final boolean sequence;
        private final String nodeName;
        private final String nodeType;
        private final Level logPayloadLevel;
        private final boolean kerberosLoginEnabled;
        private FileSystem fileSystem;


        public CopyOperation(final String nodeName, final String nodeType, final Level logPayloadLevel, final boolean deleteSourceFile, final String globalSource, final String globalDestination, final String basePath, final boolean sequence) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.logPayloadLevel = logPayloadLevel;
            this.deleteSourceFile = deleteSourceFile;
            this.globalSource = globalSource;
            this.globalDestination = globalDestination;
            this.basePath = basePath;
            this.sequence = sequence;
            this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
        }

        @Setup
        public void setup() throws Exception {
            if (kerberosLoginEnabled) performKerberosLogin();
        }

        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
            log(logPayloadLevel, nodeName, nodeType, kv);
            String key = kv.getKey();
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            String schemaId = schema.getFullName();
            counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
            String source = (String) getFieldValue(record, "source");
            if (source == null || source.isBlank()) {
                source = globalSource;
            }
            String destination = (String) getFieldValue(record, "destination");
            if (destination == null || destination.isBlank()) {
                destination = globalDestination;
            }
            if (source.contains("'") || source.contains("+")) {
                Expression expression = (new SpelExpressionParser()).parseExpression(source);
                source = evaluateSpel(key, schema, record, expression);
            }
            if (destination.contains("'") || destination.contains("+")) {
                Expression expression = (new SpelExpressionParser()).parseExpression(destination);
                destination = evaluateSpel(key, schema, record, expression);
            }
            if (!source.contains("://")) {
                source = basePath + "/" + source;
            }
            if (!destination.contains("://")) {
                destination = basePath + "/" + destination;
            }
            Path sourcePath = new Path(source);
            int sourceLength = source.length();
            FileSystem fs = fileSystem();
            Configuration conf = hadoopConfiguration();
            String suffix = sequence ? ("-" + kv.getKey()) : "";
            int successCount = 0;
            if (!fs.exists(sourcePath)) {
                LOGGER.warn("HDFS Source path doesn't exist. Nothing to do..", Map.of("sourcePath", sourcePath));
                ctx.output(kv);
                return;
            }
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(sourcePath, true);
            Map<Path, Path> mapping = new HashMap<>();
            Set<Path> targetPaths = new HashSet<>();
            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                if (!fileStatus.isFile() || fileStatus.getBlockSize() == 0) {
                    continue;
                }
                String path = fileStatus.getPath().toString();
                String destinationPath = (destination + path.substring(sourceLength)).trim();
                String[] tokens = destinationPath.split("\\.");
                Path destPath = new Path(String.format("%s%s.%s", tokens[0], suffix, tokens[1]));
                mapping.put(fileStatus.getPath(), destPath);
                targetPaths.add(destPath.getParent());
            }
            if (deleteSourceFile) {
                LOGGER.debug("Creating required parent directories for running hdfs rename", targetPaths);
                for (Path path : targetPaths) {
                    fs.mkdirs(path);
                }
            }
            LOGGER.debug("HDFS Files to be remapped", mapping);
            for (Map.Entry<Path, Path> entry : mapping.entrySet()) {
                List<MatchResult> results = FileSystems.match(asList(entry.getKey().toString()));
                if (results != null && results.stream().filter(x -> x.status() == MatchResult.Status.OK).count() >= 1) {
                    try {
                        if (deleteSourceFile) {
                            successCount += fs.rename(entry.getKey(), entry.getValue()) ? 1 : 0;
                        } else {
                            successCount += FileUtil.copy(fs, entry.getKey(), fs, entry.getValue(), false, conf) ? 1 : 0;
                        }
                    } catch (Exception e) {
                        LOGGER.trace("HDFS path doesn't exist anymore.maybe already moved", Map.of("path", entry.getKey()));
                    }
                }
            }
            LOGGER.debug("Modified HDFS paths successfully", Map.of("successCount", successCount));
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
            ctx.output(kv);
        }

        @SneakyThrows
        private FileSystem fileSystem() throws IOException {
            if (fileSystem == null) {
                fileSystem = FileSystem.get(Objects.requireNonNull(hadoopConfiguration()));
            }
            return fileSystem;
        }
    }

    public static class DeleteOperation extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
        private final List<String> globalPaths;
        private final String basePath;
        private final String nodeName;
        private final String nodeType;
        private final Level logPayloadLevel;
        private final boolean kerberosLoginEnabled;
        private FileSystem fileSystem;

        public DeleteOperation(final String nodeName, final String nodeType, final Level logPayloadLevel, final List<String> globalPaths, final String basePath) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.logPayloadLevel = logPayloadLevel;
            this.globalPaths = globalPaths;
            this.basePath = basePath;
            this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
        }

        @Setup
        public void setup() throws Exception {
            if (kerberosLoginEnabled) performKerberosLogin();
        }

        @SuppressWarnings("unchecked")
        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
            log(logPayloadLevel, nodeName, nodeType, kv);
            String key = kv.getKey();
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            String schemaId = schema.getFullName();
            counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
            List<String> paths = this.globalPaths;
            String recordPath = (String) getFieldValue(record, "paths");
            if (recordPath != null && !recordPath.isBlank()) {
                recordPath = recordPath.trim();
                if (recordPath.startsWith("[")) {
                    paths = readJson(recordPath, List.class);
                } else {
                    paths = Arrays.stream(recordPath.split(",")).distinct().collect(Collectors.toList());
                }
            }
            FileSystem fs = fileSystem();
            for (String path : paths) {
                if (path.contains("'") || path.contains("+")) {
                    Expression expression = (new SpelExpressionParser()).parseExpression(path);
                    path = evaluateSpel(key, schema, record, expression);
                }
                if (!path.contains("://")) {
                    path = basePath + "/" + path;
                }
                Path deletePath = new Path(path);
                if (!fs.exists(deletePath)) {
                    LOGGER.warn("HDFS Delete path doesn't exist. Nothing to do..", Map.of("path", deletePath));
                    continue;
                }
                try {
                    fs.delete(deletePath, true);
                } catch (Exception e) {
                    LOGGER.warn("Could not delete hdfs path", Map.of("path", deletePath));
                }
                LOGGER.debug("Deleted HDFS path successfully", Map.of("path", deletePath));
            }
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
            ctx.output(kv);
        }

        @SneakyThrows
        private FileSystem fileSystem() throws IOException {
            if (fileSystem == null) {
                fileSystem = FileSystem.get(Objects.requireNonNull(hadoopConfiguration()));
            }
            return fileSystem;
        }
    }
}
