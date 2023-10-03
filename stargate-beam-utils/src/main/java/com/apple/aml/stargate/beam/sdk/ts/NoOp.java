package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.event.Level;

import java.io.Serializable;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.utils.LogUtils.parseLevel;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static lombok.AccessLevel.NONE;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, creatorVisibility = JsonAutoDetect.Visibility.NONE)
public class NoOp extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private String nodeName;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private String nodeType;
    @Optional
    private boolean emit;
    @Optional
    private String logPayload; // logs only incoming payload
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Level logPayloadLevel;

    @SuppressWarnings("rawtypes")
    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
        this.emit = false;
    }

    @SuppressWarnings("unchecked")
    public void initCommon(final Pipeline pipeline, final StargateNode node) throws Exception {
        nodeName = node.getName();
        nodeType = node.getType();
        logPayloadLevel = parseLevel(logPayload);
    }

    @SuppressWarnings("rawtypes")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
        this.emit = true;
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(logPayloadLevel, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        if (record == null) {
            counter(nodeName, nodeType, UNKNOWN, ELEMENTS_IN).inc();
            if (emit) {
                counter(nodeName, nodeType, UNKNOWN, ELEMENTS_NULL).inc();
                ctx.output(kv);
            }
            return;
        }
        String schemaId = record.getSchema().getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        if (emit) {
            histogramDuration(nodeName, nodeType, schemaId, ELEMENTS_OUT).observe((System.nanoTime() - startTime) / 1000000.0);
            ctx.output(kv);
        }
    }
}
