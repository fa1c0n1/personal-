package com.apple.aml.stargate.app.pojo;

import com.typesafe.config.Optional;
import lombok.Data;

import java.util.Date;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
public class ACICleanupPipeline {
    private String appId;
    private String streamName;
    private String pipelineGroup;
    private String group;
    private String namespace;
    @Optional
    private String callBackUrl;
    private Date initDate;
    private String latestActivityId;
    private String uri;
    private String apiUri;
    private String apiToken;

    public String uri(final String input) throws Exception {
        if (isBlank(input)) {
            throw new Exception("Default Kafka uri could not be null or blank : " + input);
        }
        if (isBlank(this.uri)) {
            this.uri = input;
        }
        return this.uri;
    }

    public String apiUri(final String input) throws Exception {
        if (isBlank(input)) {
            throw new Exception("Default Kafka api uri could not be null or blank : " + input);
        }
        if (isBlank(this.apiUri)) {
            this.apiUri = input;
        }
        return this.apiUri;
    }

    public String apiToken(final String input) throws Exception {
        if (isBlank(input)) {
            throw new Exception("Default Kafka api token could not be null or blank : " + input);
        }
        if (isBlank(this.apiToken)) {
            this.apiToken = input;
        }
        return this.apiToken;
    }

    public void validate() throws Exception {
        if (isBlank(this.streamName)) {
            throw new Exception("Stream Name cannot be null or blank");
        }
        if (isBlank(this.pipelineGroup)) {
            throw new Exception("Pipeline Group cannot be null or blank");
        }
        if (isBlank(this.group)) {
            throw new Exception("ACI Kafka Group cannot be null or blank");
        }
        if (isBlank(this.namespace)) {
            throw new Exception("ACI Kafka Namespace cannot be null or blank");
        }
    }

    public String appId(final String appId) {
        this.appId = appId;
        return this.appId;
    }

    public String appId() {
        return this.appId;
    }

    public String latestActivityId(final String latestActivityId) {
        this.latestActivityId = latestActivityId;
        return this.latestActivityId;
    }

    public String latestActivityId() {
        return this.latestActivityId;
    }
}
