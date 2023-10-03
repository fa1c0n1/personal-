package com.apple.aml.stargate.beam.sdk.io.file;

import lombok.SneakyThrows;
import org.apache.beam.sdk.options.ValueProvider;

import java.io.Serializable;
import java.util.Objects;

import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.isDynamicKerberosLoginEnabled;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performKerberosLogin;

public class StaticPathProvider<T> implements ValueProvider<T>, Serializable {
    private final T value;
    private final boolean kerberosLoginEnabled;

    public StaticPathProvider(final T value) {
        this.value = value;
        this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
    }

    @SneakyThrows
    @Override
    public T get() {
        if (kerberosLoginEnabled) {
            performKerberosLogin(); // TODO: Very bad way.. lets first get it working!!!
        }
        return value;
    }

    @Override
    public boolean isAccessible() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof StaticPathProvider && Objects.equals(value, ((StaticPathProvider) other).value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
