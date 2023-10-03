package com.apple.aml.stargate.common.web.spec;

import com.apple.aml.stargate.common.pojo.KeyValuePair;

import java.util.List;
import java.util.Map;

public interface ConfigServiceSpec {
    List<KeyValuePair> getConfigProperties(final String application, final String module, final String profile) throws Exception;

    <O> O getConfigResource(final String application, final String module, final String profile, final String resourceName, final Class<O> readAs) throws Exception;

    Map getResourceMetadata(final String application, final String module, final String profile) throws Exception;

    Map uploadResource(final String application, final String module, final String profile, final String targetFileName, final Object resource) throws Exception;

    Map mergeResource(final String application, final String module, final String profile, final String targetFileName, final Object resource) throws Exception;

    Map removeConfigSet(final String application, final String module, final String profile) throws Exception;

    default Map createConfigSetWithProperties(final String application, final String module, final String profile, final Map<String, Object> properties) throws Exception {
        createConfigSetIfNotExists(application, module, profile, "", "");
        Map response = mergeConfigProperties(application, module, profile, properties);
        return response;
    }

    default boolean createConfigSetIfNotExists(final String application, final String module, final String profile, final String readGroupId, final String writeGroupId) throws Exception {
        Map configSet = getConfigSet(application, module, profile);
        if (configSet != null) {
            return true;
        }
        Map create = createConfigSet(application, module, profile, readGroupId, writeGroupId);
        return create != null && !create.isEmpty();
    }

    @SuppressWarnings("unchecked")
    default Map mergeConfigProperties(final String application, final String module, final String profile, final Map<String, Object> properties) throws Exception {
        Map existingProperties = getConfigPropertiesMap(application, module, profile, true);
        if (existingProperties == null) {
            return createConfigProperties(application, module, profile, properties);
        }
        if (existingProperties.entrySet().containsAll(properties.entrySet())) {
            return null;
        }
        existingProperties.putAll(properties);
        return createConfigProperties(application, module, profile, existingProperties);
    }

    Map getConfigSet(final String application, final String module, final String profile) throws Exception;

    Map createConfigSet(final String application, final String module, final String profile, final String readGroupId, final String writeGroupId) throws Exception;

    Map<String, Object> getConfigPropertiesMap(final String application, final String module, final String profile, final boolean asIs) throws Exception;

    Map createConfigProperties(final String application, final String module, final String profile, final Map<String, Object> properties) throws Exception;
}
