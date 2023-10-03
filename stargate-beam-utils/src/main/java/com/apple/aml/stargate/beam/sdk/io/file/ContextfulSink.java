package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.SPECIAL_DELIMITER_CAP_REGEX;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;

public class ContextfulSink implements SerializableFunction<String, FileIO.Sink<KV<String, GenericRecord>>> {
    private final DATA_FORMAT fileType;
    private final CompressionCodecName codecName;
    private final String schemaReference;
    private final String nodeName;
    private final String nodeType;
    private transient ConcurrentHashMap<String, Schema> schemaMap;

    public ContextfulSink(final String nodeName, final String nodeType, final DATA_FORMAT fileType, final String schemaReference, final String compression) {
        this.fileType = fileType;
        this.schemaReference = schemaReference;
        this.codecName = compression == null || compression.isBlank() ? CompressionCodecName.SNAPPY : CompressionCodecName.valueOf(compression.trim().toUpperCase());
        this.nodeName = nodeName;
        this.nodeType = nodeType;
    }

    public static Contextful<Contextful.Fn<String, FileIO.Sink<KV<String, GenericRecord>>>> sink(final String nodeName, final String nodeType, final DATA_FORMAT fileType, final String schemaReference, final String compression) {
        return Contextful.fn(new ContextfulSink(nodeName, nodeType, fileType, schemaReference, compression));
    }

    @Override
    public FileIO.Sink<KV<String, GenericRecord>> apply(final String input) {
        if (schemaMap == null) {
            schemaMap = new ConcurrentHashMap<>();
        }
        String[] tokens = input.split(SPECIAL_DELIMITER_CAP_REGEX);
        String schemaId = tokens[2];
        Schema schema = schemaMap.computeIfAbsent(schemaId, sid -> fetchSchemaWithLocalFallback(schemaReference, sid));
        return new DynamicSink(nodeName, nodeType, fileType, schema, codecName);
    }
}
