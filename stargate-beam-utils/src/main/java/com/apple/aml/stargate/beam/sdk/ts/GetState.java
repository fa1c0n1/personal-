package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.common.web.clients.PipelineConfigClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.readNullableJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;

public class GetState extends StateOps implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("deprecation")
    @Override
    public Map process(final String pipelineId, final String pipelineToken, final String stateId, final Map state, final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) throws Exception {
        List states = PipelineConfigClient.getState(pipelineId, pipelineToken, stateId);
        if (this.evaluate == null) {
            return Map.of("states", states == null ? Collections.emptyList() : states);
        }
        String outputString = evaluateFreemarker(configuration(), this.evalTemplateName, kv.getKey(), kv.getValue(), schema, "states", states == null ? Collections.emptyList() : states);
        Map returnMap = readNullableJsonMap(outputString);
        return returnMap;
    }

    @Override
    protected boolean useProcessResponse() {
        return true;
    }
}
