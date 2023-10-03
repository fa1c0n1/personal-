package com.apple.aml.stargate.beam.sdk.io.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RawMessage implements Serializable {
    protected String topic;
    protected List<KV<String, byte[]>> headers;
    protected byte[] bytes;
}
