package com.apple.aml.stargate.common.utils.jackson.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.avro.util.Utf8;

import java.io.IOException;

public class Utf8Serializer extends StdSerializer<Utf8> {
    public Utf8Serializer() {
        this(null);
    }

    public Utf8Serializer(Class<Utf8> t) {
        super(t);
    }

    @Override
    public void serialize(final Utf8 value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
        gen.writeString(value.toString());
    }
}