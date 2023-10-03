package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, creatorVisibility = JsonAutoDetect.Visibility.NONE)
public final class Log extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = logger(MethodHandles.lookup().lookupClass());
    @Optional
    @JsonIgnore
    private String nodeName;
    @Optional
    @JsonIgnore
    private String nodeType;

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        nodeName = node.getName();
        nodeType = node.getType();
        return collection.apply(nodeName, this);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        nodeName = node.getName();
        nodeType = node.getType();
        return collection.apply(nodeName, this);
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) {
        GenericRecord record = kv.getValue();
        String schemaId = record == null ? UNKNOWN : record.getSchema().getFullName();
        counter(nodeName, nodeType, schemaId, "elements_logged").inc();
        LOG.info(EMPTY_STRING, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "key", String.valueOf(kv.getKey()), "record", record == null ? EMPTY_STRING : record));
        ctx.output(kv);
    }
}
