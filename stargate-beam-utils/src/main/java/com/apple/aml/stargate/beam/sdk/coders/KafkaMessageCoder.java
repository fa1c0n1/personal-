package com.apple.aml.stargate.beam.sdk.coders;

import com.apple.aml.stargate.beam.sdk.io.kafka.KafkaMessage;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.apple.aml.stargate.beam.sdk.coders.RawMessageCoder.decodeRawMessage;
import static com.apple.aml.stargate.beam.sdk.coders.RawMessageCoder.encodeRawMessage;

public class KafkaMessageCoder extends AtomicCoder<KafkaMessage> {
    private static final long serialVersionUID = 1L;
    private final StringUtf8Coder stringUtf8Coder;
    private final VarIntCoder intCoder;

    private final VarLongCoder longCoder;
    private final KvCoder<String, byte[]> headerCoder;
    private final ByteArrayCoder byteArrayCoder;
    private final ListCoder<KV<String, byte[]>> nonNullHeadersCoder;
    private final ListCoder<String> nullHeadersCoder;

    private KafkaMessageCoder() {
        this.stringUtf8Coder = StringUtf8Coder.of();
        this.intCoder = VarIntCoder.of();
        this.longCoder = VarLongCoder.of();
        this.byteArrayCoder = ByteArrayCoder.of();
        this.headerCoder = KvCoder.of(stringUtf8Coder, byteArrayCoder);
        this.nonNullHeadersCoder = ListCoder.of(headerCoder);
        this.nullHeadersCoder = ListCoder.of(stringUtf8Coder);
    }

    public static Coder<KafkaMessage> of() {
        return new KafkaMessageCoder();
    }

    @Override
    public void encode(final KafkaMessage value, final OutputStream stream) throws CoderException, IOException {
        intCoder.encode(value.getPartition(), stream);
        longCoder.encode(value.getOffset(), stream);
        encodeRawMessage(value, stream, stringUtf8Coder, nonNullHeadersCoder, nullHeadersCoder, byteArrayCoder);
    }

    @Override
    public KafkaMessage decode(final InputStream stream) throws CoderException, IOException {
        KafkaMessage message = new KafkaMessage();
        message.setPartition(intCoder.decode(stream));
        message.setOffset(longCoder.decode(stream));
        return decodeRawMessage(message, stream, stringUtf8Coder, nonNullHeadersCoder, nullHeadersCoder, byteArrayCoder);
    }
}

