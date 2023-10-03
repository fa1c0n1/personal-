package com.apple.aml.stargate.app.config;

import com.apple.aml.stargate.common.options.HttpConnectionOptions;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;

@Configuration
@ConfigurationProperties(prefix = "stargate.defaults")
@Data
public class AppDefaults {
    private LinkedHashMap<String, String> logHeadersMapping;
    private long consumerPollDuration;
    private String kafkaDefaultClientId;
    private HttpConnectionOptions connectionOptions;
}
