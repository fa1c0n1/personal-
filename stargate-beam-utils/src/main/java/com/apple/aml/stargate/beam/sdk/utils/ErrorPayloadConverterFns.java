package com.apple.aml.stargate.beam.sdk.utils;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.pipeline.sdk.ts.BaseErrorPayloadConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ErrorPayloadConverterFns extends DoFn<KV<String, ErrorRecord>, KV<String, GenericRecord>> {

    private BaseErrorPayloadConverter baseErrorPayloadConverter;

    public ErrorPayloadConverterFns(final String errorNodeName, final PipelineConstants.ENVIRONMENT environment){
        this.baseErrorPayloadConverter= new BaseErrorPayloadConverter(errorNodeName,environment);
    }

    @ProcessElement
    public void processElement(final @Element KV<String, ErrorRecord> kv, final ProcessContext ctx) {
        ctx.output(baseErrorPayloadConverter.convert(kv));
    }
}
