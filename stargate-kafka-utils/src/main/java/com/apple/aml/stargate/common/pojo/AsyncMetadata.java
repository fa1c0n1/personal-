package com.apple.aml.stargate.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;

import static com.apple.aml.stargate.common.utils.AsyncPersistenceProvider.PAYLOAD_UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AsyncMetadata implements Serializable {
    private byte[] uuid;
    private long appId;
    private String publisherId;
    private String payloadKey;
    private String metricAppId;
    private Map<String, ?> logHeaders;
    private String schemaId;
    private int partition;
    private String publishFormat;
    private Map<String, String> headers;
    private long receivedOn; // epoc milliseconds
    private long pickedOn; // epoc milliseconds
    private String compression; // payload compressionType
    private int retryCount;
    private boolean validAuth;
    private LinkedHashSet<String> attemptedPublishers;
    private AsyncMetadata errorMetadata;

    public static UUID bytesToUUID(final byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        long high = byteBuffer.getLong();
        long low = byteBuffer.getLong();
        return new UUID(high, low);
    }

    public UUID UUID() {
        return bytesToUUID(this.uuid);
    }

    public String uuidString() {
        return UUID().toString();
    }

    public Map toMap() {
        return Map.of(PAYLOAD_UUID, uuidString(), "appId", appId, "publisherId", String.valueOf(publisherId), "payloadKey", String.valueOf(payloadKey));
    }

}
