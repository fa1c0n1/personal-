package com.apple.aml.stargate.app.pojo;

import lombok.Data;

import java.util.Date;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
public class ModelCleanupPipeline {
    private String appId;
    private String srcTopic;
    private boolean enableDirectAccess = true;
    private boolean enableEncryption;
    private Boolean autoCreateTopic = true;
    private String targetClientId;
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
        if (isBlank(this.getSrcTopic())) {
            throw new Exception("Source Topic cannot be null or blank");
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

