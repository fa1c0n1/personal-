package com.apple.aml.stargate.cache.utils;

import com.apple.aml.stargate.common.utils.AppConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.lang.IgniteProductVersion;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class IgniteFailureHandler extends AbstractFailureHandler {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static String HIGHLIGHTER = StringUtils.repeat("!", 50);

    @Override
    protected boolean handle(final Ignite ignite, final FailureContext context) {
        LOGGER.error(HIGHLIGHTER);
        LOGGER.error("Ignite error detected !!", Map.of(ERROR_MESSAGE, context.toString(), "failureType", context.type()), context.error());
        LOGGER.error(HIGHLIGHTER);
        IgniteCluster cluster = ignite.cluster();
        IgniteProductVersion version = ignite.version();
        Collection<String> cacheNames = ignite.cacheNames();
        String baselineAutoAdjustStatus = cluster.baselineAutoAdjustStatus() == null ? "NULL" : cluster.baselineAutoAdjustStatus().getTaskState().name();
        Map metrics;
        try {
            metrics = readJsonMap(jsonString(cluster.localNode().metrics()));
        } catch (Exception ignored) {
            metrics = Map.of();
        }
        LOGGER.error("Ignite state as of now", metrics, Map.of("clusterId", cluster.id().toString(), "version", version.toString(), "state", cluster.state().name(), "cacheNames", cacheNames, "baselineAutoAdjustStatus", baselineAutoAdjustStatus));
        if (AppConfig.config().getBoolean("ignite.haltOnFailure")) {
            LOGGER.error("Ignite halt on failure enabled. Will halt JVM !!", Map.of(ERROR_MESSAGE, context.toString(), "failureType", context.type()), context.error());
            Runtime.getRuntime().halt(Ignition.KILL_EXIT_CODE);
        } else {
            LOGGER.error("Not halting the JVM. Please analyze this error and act accordingly !!!");
        }
        return true;
    }
}
