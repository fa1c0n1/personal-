package com.apple.aml.stargate.common.configs;

import com.google.common.base.CaseFormat;
import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;

import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Integer.parseInt;

@Data
public class EnvironmentConfig implements Serializable {
    public static final String CONFIG_NAME = "stargate.environment.config";
    private static final long serialVersionUID = 1L;
    @Optional
    private String aciKafkaUri;
    @Optional
    private String schemaReference;
    @Optional
    private String hubbleTcpEndpoint;
    @Optional
    private String hubbleHttpEndpoint;
    @Optional
    private String cloudConfigApiUri;
    @Optional
    private String appConfigApiUrl;
    @Optional
    private String dhariUri;
    @Optional
    private String stargateUri;
    @Optional
    private String s3EndPoint;
    @Optional
    private String idmsUrl;
    @Optional
    private String acsDataPlatformUri;
    @Optional
    private String pieSecretsUri;
    @Optional
    private String aciGitHubUri;
    @Optional
    private String sparkPieTelemetryHubbleCluster;
    @Optional
    private String splunkCluster;
    @Optional
    private String stargateSplunkIndex;
    @Optional
    private String stratosQueueServer;
    @Optional
    private String dataCatalogUri;

    public void init(final String mode) throws Exception {
        Arrays.stream(EnvironmentConfig.class.getDeclaredFields()).filter(f -> f.getType().equals(String.class)).forEach(field -> {
            String envVariable = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, field.getName());
            String modeVariable = "APP_" + mode + "_" + envVariable;
            String genericVariable = "APP_ENV_" + envVariable;
            try {
                field.set(this, env(modeVariable, env(genericVariable, (String) field.get(this))));
            } catch (Exception e) {
            }
        });
        Arrays.stream(EnvironmentConfig.class.getDeclaredFields()).filter(f -> f.getType().equals(Integer.class)).forEach(field -> {
            String envVariable = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, field.getName());
            String modeVariable = "APP_" + mode + "_" + envVariable;
            String genericVariable = "APP_ENV_" + envVariable;
            try {
                field.setInt(this, parseInt(env(modeVariable, env(genericVariable, String.valueOf(field.getInt(this))))));
            } catch (Exception e) {
            }
        });
    }

    private static String env(final String propertyName, final String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (!isBlank(propertyValue)) {
            return propertyValue.trim();
        }
        propertyValue = System.getenv(propertyName);
        if (!isBlank(propertyValue)) {
            return propertyValue.trim();
        }
        return defaultValue;
    }
}
