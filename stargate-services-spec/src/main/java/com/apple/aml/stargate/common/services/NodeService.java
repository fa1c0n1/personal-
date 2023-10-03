package com.apple.aml.stargate.common.services;

public interface NodeService {
    String nodeName();

    String nodeType();

    String inputSchemaId();

    default void emit(final Object object) throws Exception {
        emit(null, null, object);
    }

    void emit(final String schemaId, final String key, final Object object) throws Exception;

    default void emit(final String schemaId, final Object object) throws Exception {
        emit(schemaId, null, object);
    }
}
