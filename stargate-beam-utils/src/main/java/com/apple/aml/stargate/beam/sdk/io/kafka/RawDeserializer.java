package com.apple.aml.stargate.beam.sdk.io.kafka;

import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TOPIC;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_NODE_TYPE;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramBytes;


public class RawDeserializer implements Deserializer<RawMessage>, Serializable {
    private String nodeName = UNKNOWN;
    private String nodeType = UNKNOWN;

    @Override
    @SuppressWarnings({"unchecked"})
    public void configure(final Map<String, ?> iconfigs, final boolean isKey) {
        Map<String, Object> configs = (Map<String, Object>) iconfigs;
        nodeName = (String) configs.getOrDefault(CONFIG_NODE_NAME, UNKNOWN);
        nodeType = (String) configs.getOrDefault(CONFIG_NODE_TYPE, UNKNOWN);
    }

    @Override
    public RawMessage deserialize(final String topic, final byte[] bytes) {
        return deserialize(topic, null, bytes);
    }

    @Override
    public RawMessage deserialize(final String topic, final Headers headers, final byte[] bytes) {
        histogramBytes(nodeName, nodeType, UNKNOWN, "raw_msg_size", TOPIC, topic).observe(bytes.length);
        RawMessage message = new RawMessage();
        message.setTopic(topic);
        message.setBytes(bytes);
        message.setHeaders(headerKVs(headers));
        return message;
    }

    static List<KV<String, byte[]>> headerKVs(final Headers headers) {
        List<KV<String, byte[]>> headerList;
        if (headers == null) {
            headerList = new ArrayList<>(1);
        } else {
            headerList = new ArrayList<>();
            headers.forEach(header -> headerList.add(KV.of(header.key(), header.value())));
        }
        return headerList;
    }
}
