package com.apple.aml.stargate.beam.sdk.io.hadoop;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public final class HdfsConfiguration extends Configuration {
    public Properties getAllProperties() {
        return this.getProps();
    }
}
