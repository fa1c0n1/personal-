package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class StratosOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private String appId;
    private String topic;
    private String siteId;
    @Optional
    private String consumerAppName;
    @Optional
    private String serviceEnvironment;
    @Optional
    private String genevaEnvironment;
    @Optional
    private String server;
    @Optional
    private String context;
    @Optional
    private String authType = "a3";
    @Optional
    private String cert;
    @Optional
    private String certPassword;
    @Optional
    private String keystorePassword;
    @Optional
    private long a3AppId;
    @Optional
    private String a3Password;
    @Optional
    private String a3Mode;
    @Optional
    private String autoAcknowledge;

    public String context() {
        return isBlank(this.context) ? "default" : this.context;
    }

    public String consumerAppName() {
        if (isBlank(this.consumerAppName)) return A3Constants.KNOWN_APP.STARGATE.name().toLowerCase();
        return this.consumerAppName;
    }

    public String serviceEnvironment() {
        if (isBlank(this.serviceEnvironment)) return AppConfig.environment() == PipelineConstants.ENVIRONMENT.PROD ? "PROD" : "UAT";
        return this.serviceEnvironment;
    }

    public String genevaEnvironment() {
        if (isBlank(this.genevaEnvironment)) return AppConfig.environment() == PipelineConstants.ENVIRONMENT.PROD ? "PROD" : "UAT";
        return this.genevaEnvironment;
    }
}
