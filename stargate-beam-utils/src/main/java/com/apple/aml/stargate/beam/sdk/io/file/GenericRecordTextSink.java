package com.apple.aml.stargate.beam.sdk.io.file;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.function.Function;

public final class GenericRecordTextSink implements FileIO.Sink<GenericRecord> {
    private final TextIO.Sink sink;
    private final Function<GenericRecord, String> func;

    public GenericRecordTextSink(final Function<GenericRecord, String> writerFunc) {
        sink = TextIO.sink();
        this.func = writerFunc;
    }

    public GenericRecordTextSink(final String header, final Function<GenericRecord, String> writerFunc) {
        sink = TextIO.sink().withHeader(header);
        this.func = writerFunc;
    }

    @Override
    public void open(final WritableByteChannel channel) throws IOException {
        sink.open(channel);
    }

    @Override
    public void write(final GenericRecord element) throws IOException {
        String value = func.apply(element);
        sink.write(value);
    }

    @Override
    public void flush() throws IOException {
        sink.flush();
    }
}
