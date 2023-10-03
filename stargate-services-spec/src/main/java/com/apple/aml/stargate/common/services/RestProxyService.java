package com.apple.aml.stargate.common.services;

import java.util.Map;

public interface RestProxyService {
    default <O> O invoke(final String serviceId, final Object payload, final Class<O> returnType) {
        return invoke(serviceId, payload, null, returnType, true);
    }

    default <O> O invoke(final String serviceId, final Object payload, final Map<String, String> headers, final Class<O> returnType) {
        return invoke(serviceId, payload, headers, returnType, true);
    }

    <O> O invoke(final String serviceId, final Object payload, final Map<String, String> headers, final Class<O> returnType, final boolean throwOnError);

    RestService service(final String serviceId);
}
