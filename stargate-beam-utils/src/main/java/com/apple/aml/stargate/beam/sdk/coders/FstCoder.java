package com.apple.aml.stargate.beam.sdk.coders;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class FstCoder<T> extends CustomCoder<T> {
    private static final FSTConfiguration CONF = FSTConfiguration.createDefaultConfiguration();
    private final Class<T> clazz;

    private FstCoder(Class<T> clazz) {
        this.clazz = clazz;
        CONF.registerClass(clazz);
    }

    public static <T> FstCoder<T> of(Class<T> clazz) {
        return new FstCoder<T>(clazz);
    }

    @Override
    public void encode(final T value, @UnknownKeyFor @NonNull @Initialized final OutputStream outStream) throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
        final FSTObjectOutput out = CONF.getObjectOutput(outStream);
        out.writeObject(value, clazz);
        out.flush();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
        try {
            final FSTObjectInput in = CONF.getObjectInput(inStream);
            return (T) in.readObject(clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
