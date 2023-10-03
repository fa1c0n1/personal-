package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.common.web.clients.PipelineConfigClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class SaveState extends StateOps implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    @Override
    public Map process(final String pipelineId, final String pipelineToken, final String stateId, final Map state, final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) throws Exception {
        Map status = PipelineConfigClient.saveState(pipelineId, pipelineToken, stateId, state);
        LOGGER.debug("State saved successfully", status, Map.of("stateId", stateId, NODE_NAME, nodeName, "key", kv.getKey()));
        return status;
    }
}
