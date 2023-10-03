package com.apple.aml.stargate.common.services;

import java.util.List;
import java.util.Map;

public interface ShuriOfsService extends AttributeService {
    default <O> O readFeatureGroup(final String featureName, final String entityId) {
        return readFeatureGroup(null, featureName, entityId);
    }

    default <O> O readFeatureField(final String featureName, final String entityId, final String fieldName) {
        return readFeatureField(null, featureName, entityId, fieldName);
    }

    default <O> O readFeatureGroups(final String featureName, final List<String> entityIds) {
        return readFeatureGroups(null, featureName, entityIds);
    }

    default boolean writeFeatureGroup(final String featureName, final String entityId, final Map<String, ?> data) {
        return writeFeatureGroup(featureName, entityId, data, -1);
    }

    default boolean writeFeatureGroup(final String featureName, final String entityId, final Map<String, ?> data, final int ttl) {
        return writeFeatureGroup(null, featureName, entityId, data, ttl);
    }

    default boolean deleteFeatureGroup(final String featureName, final String entityId) {
        return deleteFeatureGroup(null, featureName, entityId);
    }

    default boolean deleteFeatureField(final String featureName, final String entityId, final String fieldName) {
        return deleteFeatureField(null, featureName, entityId, fieldName);
    }

    <O> O readFeatureGroup(final String namespace, final String featureName, final String entityId);

    <O> O readFeatureField(final String namespace, final String featureName, final String entityId, final String fieldName);

    <O> O readFeatureGroups(final String namespace, final String featureName, final List<String> entityIds);

    boolean writeFeatureGroup(final String namespace, final String featureName, final String entityId, final Map<String, ?> data, final int ttl);

    boolean deleteFeatureGroup(final String namespace, final String featureName, final String entityId);

    boolean deleteFeatureField(final String namespace, final String featureName, final String entityId, final String fieldName);
}
