package com.apple.aml.stargate.common.web.clients;

import com.apple.aml.stargate.common.pojo.KeyValuePair;
import com.apple.aml.stargate.common.web.spec.ConfigServiceSpec;

import java.util.List;
import java.util.Map;

public final class ConfigClient {
    private static final ConfigServiceSpec client = new CCClient();

    public static List<KeyValuePair> getConfigProperties(final String application, final String module, final String profile) throws Exception {
        return client.getConfigProperties(application, module, profile);
    }

    public static <O> O getConfigResource(final String application, final String module, final String profile, final String resourceName, final Class<O> readAs) throws Exception {
        return client.getConfigResource(application, module, profile, resourceName, readAs);
    }

    public static Map getResourceMetadata(final String application, final String module, final String profile) throws Exception {
        return client.getResourceMetadata(application, module, profile);
    }

    public static Map uploadResource(final String application, final String module, final String profile, final String targetFileName, final Object resource) throws Exception {
        return client.uploadResource(application, module, profile, targetFileName, resource);
    }

    public static Map mergeResource(final String application, final String module, final String profile, final String targetFileName, final Object resource) throws Exception {
        return client.mergeResource(application, module, profile, targetFileName, resource);
    }

    public static Map<String, Object> getConfigPropertiesMap(final String application, final String module, final String profile, final boolean asIs) throws Exception {
        return client.getConfigPropertiesMap(application, module, profile, asIs);
    }

    public static Map getConfigSet(final String application, final String module, final String profile) throws Exception {
        return client.getConfigSet(application, module, profile);
    }

    public static Map createConfigSet(final String application, final String module, final String profile, final String readGroupId, final String writeGroupId) throws Exception {
        return client.createConfigSet(application, module, profile, readGroupId, writeGroupId);
    }

    public static boolean createConfigSetIfNotExists(final String application, final String module, final String profile, final String readGroupId, final String writeGroupId) throws Exception {
        return client.createConfigSetIfNotExists(application, module, profile, readGroupId, writeGroupId);
    }

    public static Map removeConfigSet(final String application, final String module, final String profile) throws Exception {
        return client.removeConfigSet(application, module, profile);
    }

    public static Map createConfigProperties(final String application, final String module, final String profile, final Map<String, Object> properties) throws Exception {
        return client.createConfigProperties(application, module, profile, properties);
    }

    public static Map mergeConfigProperties(final String application, final String module, final String profile, final Map<String, Object> properties) throws Exception {
        return client.mergeConfigProperties(application, module, profile, properties);
    }

    public static Map createConfigSetWithProperties(final String application, final String module, final String profile, final Map<String, Object> properties) throws Exception {
        return client.createConfigSetWithProperties(application, module, profile, properties);
    }
}
