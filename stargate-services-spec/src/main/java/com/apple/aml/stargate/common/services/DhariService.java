package com.apple.aml.stargate.common.services;

import java.util.Map;

public interface DhariService {

    default Map<String, ?> ingest(final Object payload) throws Exception {
        return ingest(null, null, null, payload);
    }

    default Map<String, ?> ingest(final String publisherId, final String schemaId, final String payloadKey, final Object payload) throws Exception {
        return ingest(publisherId, schemaId, payloadKey, payload, null);
    }

    Map<String, ?> ingest(final String publisherId, final String schemaId, final String payloadKey, final Object payload, final Map<String, String> headers) throws Exception;

    default Map<String, ?> ingest(final String schemaId, final Object payload) throws Exception {
        return ingest(null, schemaId, null, payload);
    }

    default Map<String, ?> asyncIngest(final Object payload) throws Exception {
        return asyncIngest(null, null, null, payload);
    }

    default Map<String, ?> asyncIngest(final String publisherId, final String schemaId, final String payloadKey, final Object payload) throws Exception {
        return asyncIngest(publisherId, schemaId, payloadKey, payload, null);
    }

    Map<String, ?> asyncIngest(final String publisherId, final String schemaId, final String payloadKey, final Object payload, final Map<String, String> headers) throws Exception;

    default Map<String, ?> asyncIngest(final String schemaId, final Object payload) throws Exception {
        return asyncIngest(null, schemaId, null, payload);
    }

    default Map<String, ?> ingest(final String schemaId, final String payloadKey, final Object payload) throws Exception {
        return ingest(null, schemaId, payloadKey, payload);
    }

    default Map<String, ?> asyncIngest(final String schemaId, final String payloadKey, final Object payload) throws Exception {
        return asyncIngest(null, schemaId, payloadKey, payload);
    }

}
