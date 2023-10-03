package com.apple.aml.stargate.common.configs;

import com.typesafe.config.Optional;
import lombok.Data;

@Data
public class VersionInfo {
    @Optional
    private String version;
    @Optional
    private String tag;
    @Optional
    private String buildTime;
}
