package com.apple.aml.stargate.common.services;

import java.util.Map;

public interface RestService {

    default <O> O invoke(final Object payload, final Class<O> returnType) {
        return invoke(payload, null, returnType, true);
    }

    default <O> O invoke(final Object payload, final Map<String, String> headers, final Class<O> returnType) {
        return invoke(payload, headers, returnType, true);
    }

    <O> O invoke(final Object payload, final Map<String, String> headers, final Class<O> returnType, final boolean throwOnError);
}
