package com.apple.aml.stargate.common.configs;

import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;

@Data
public class KnownAppConfig implements Serializable {
    public static final String CONFIG_NAME = "stargate.knownApps.config";
    private static final long serialVersionUID = 1L;
    private long appId;
    private String appName;
    @Optional
    private String contextString;
    @Optional
    private Long contextVersion;

    public void init() throws Exception {

    }
}
