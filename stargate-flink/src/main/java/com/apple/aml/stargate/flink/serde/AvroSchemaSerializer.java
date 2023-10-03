package com.apple.aml.stargate.flink.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;


public class AvroSchemaSerializer extends Serializer<Schema> {

    private static final ConcurrentHashMap<String, byte[]> schemaCache = new ConcurrentHashMap<>();

    public AvroSchemaSerializer(){

    }

    @Override
    public void write(Kryo kryo, Output output, Schema object) {
        String schemaId= object.getFullName();
        int version = SCHEMA_LATEST_VERSION;
        String schemaKey = String.format("%s%s%d", schemaId, SCHEMA_DELIMITER, version);
        schemaCache.putIfAbsent(schemaKey,object.toString().getBytes(StandardCharsets.UTF_8));
        byte[] schemaKeyBytes = schemaKey.getBytes(StandardCharsets.UTF_8);
        output.writeInt(schemaKeyBytes.length, true);
        output.writeBytes(schemaKeyBytes);
    }

    @Override
    public Schema read(Kryo kryo, Input input, Class<? extends Schema> type) {
        int length = input.readInt(true);
        byte[] schemaKeyBytes = input.readBytes(length);
        byte[] schemaBytes = schemaCache.get(new String(schemaKeyBytes,StandardCharsets.UTF_8));
        String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
        return new Schema.Parser().parse(schemaJson);
    }
}