package com.apple.aml.stargate.common.utils;

import com.apple.ihubble.client.stats.HubbleAgent;
import com.typesafe.config.Config;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_HUBBLE_PUBLISH_TOKEN;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class MetricUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private MetricUtils() {

    }

    public static HubbleAgent getHubbleAgent() {
        HubbleAgent agent = HubbleAgent.getAvailableInstance();
        if (agent != null) {
            return agent;
        }
        Properties properties = AppConfig.properties("hubble.apps.path", "hubble.apps.path");
        Config config = AppConfig.config();
        String hubblePublishKey = env("APP_METRICS_HUBBLE_PUBLISH_KEY", (config.hasPath("hubble.channel.http.auth.token") ? config.getString("hubble.channel.http.auth.token") : null));
        if (isBlank(hubblePublishKey)) {
            hubblePublishKey = env(STARGATE_HUBBLE_PUBLISH_TOKEN, null);
        }
        if (isBlank(hubblePublishKey)) {
            properties.put("hubble.publisher.tcp.server.url", environment().getConfig().getHubbleTcpEndpoint());
            properties.put("hubble.channel.server.uri", environment().getConfig().getHubbleTcpEndpoint());
            LOGGER.debug("Hubble properties are set to", properties);
        } else {
            LOGGER.debug("HTTP based Hubble is enabled. Will set corresponding properties");
            properties.put("hubble.channel.server.uri", environment().getConfig().getHubbleHttpEndpoint());
            LOGGER.debug("Hubble properties are set to", properties);
            properties.put("hubble.channel.http.auth.token", hubblePublishKey);
        }
        agent = HubbleAgent.getInstance(properties);
        agent.initialize();
        LOGGER.info("HubbleAgent initialized successfully..");
        return agent;
    }
}
