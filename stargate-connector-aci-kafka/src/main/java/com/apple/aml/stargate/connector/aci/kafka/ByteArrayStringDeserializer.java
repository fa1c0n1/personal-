package com.apple.aml.stargate.connector.aci.kafka;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteArrayStringDeserializer implements Deserializer<String> {
    private Charset charset = UTF_8;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding.charset" : "value.deserializer.encoding.charset";
        Object charsetValue = configs.get(propertyName);
        if (charsetValue == null) charsetValue = configs.get("deserializer.encoding.charset");
        if (charsetValue != null) {
            this.charset = (charsetValue instanceof Charset) ? (Charset) charsetValue : Charset.forName(String.valueOf(charsetValue));
        }
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        return String.format("{\"data\":\"%s\"}", new String(Base64.getEncoder().encode(data), charset));
    }
}

