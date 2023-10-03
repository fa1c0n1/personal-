package com.apple.aml.stargate.beam.sdk.coders;

import com.apple.aml.stargate.beam.sdk.io.kafka.RawMessage;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RawMessageCoder extends AtomicCoder<RawMessage> {
    private static final long serialVersionUID = 1L;
    private final StringUtf8Coder stringUtf8Coder;
    private final KvCoder<String, byte[]> headerCoder;
    private final ByteArrayCoder byteArrayCoder;
    private final ListCoder<KV<String, byte[]>> nonNullHeadersCoder;
    private final ListCoder<String> nullHeadersCoder;

    private RawMessageCoder() {
        this.stringUtf8Coder = StringUtf8Coder.of();
        this.byteArrayCoder = ByteArrayCoder.of();
        this.headerCoder = KvCoder.of(stringUtf8Coder, byteArrayCoder);
        this.nonNullHeadersCoder = ListCoder.of(headerCoder);
        this.nullHeadersCoder = ListCoder.of(stringUtf8Coder);
    }

    public static Coder<RawMessage> of() {
        return new RawMessageCoder();
    }


    @Override
    public void encode(final RawMessage value, final OutputStream stream) throws CoderException, IOException {
        encodeRawMessage(value, stream, stringUtf8Coder, nonNullHeadersCoder, nullHeadersCoder, byteArrayCoder);
    }

    static void encodeRawMessage(final RawMessage value, final OutputStream stream, final StringUtf8Coder stringUtf8Coder, final ListCoder<KV<String, byte[]>> nonNullHeadersCoder, final ListCoder<String> nullHeadersCoder, final ByteArrayCoder byteArrayCoder) throws IOException {
        stringUtf8Coder.encode(value.getTopic(), stream);
        nonNullHeadersCoder.encode(value.getHeaders().stream().filter(kv -> kv.getValue() != null).collect(Collectors.toList()), stream);
        nullHeadersCoder.encode(value.getHeaders().stream().filter(kv -> kv.getValue() == null).map(kv -> kv.getKey()).collect(Collectors.toList()), stream);
        byteArrayCoder.encode(value.getBytes(), stream);
    }

    @Override
    public RawMessage decode(final InputStream stream) throws CoderException, IOException {
        return decodeRawMessage(new RawMessage(), stream, stringUtf8Coder, nonNullHeadersCoder, nullHeadersCoder, byteArrayCoder);
    }

    static <M extends RawMessage> M decodeRawMessage(final M message, final InputStream stream, final StringUtf8Coder stringUtf8Coder, final ListCoder<KV<String, byte[]>> nonNullHeadersCoder, final ListCoder<String> nullHeadersCoder, final ByteArrayCoder byteArrayCoder) throws CoderException, IOException {
        message.setTopic(stringUtf8Coder.decode(stream));
        List<KV<String, byte[]>> headers = new ArrayList<>();
        headers.addAll(nonNullHeadersCoder.decode(stream));
        nullHeadersCoder.decode(stream).stream().map(k -> KV.of(k, (byte[]) null)).forEach(kv -> headers.add(kv));
        message.setHeaders(headers);
        message.setBytes(byteArrayCoder.decode(stream));
        return message;
    }
}
