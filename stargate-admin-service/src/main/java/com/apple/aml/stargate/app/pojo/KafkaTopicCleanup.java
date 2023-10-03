package com.apple.aml.stargate.app.pojo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class KafkaTopicCleanup {
    private String topic;
    private String group;
    private String namespace;
    private String apiUri;
    private String apiToken;
    private List<String> clients;
    private String latestActivityId;

    public KafkaTopicCleanup(final String group, final String namespace, final String topic, final String apiUri, final String apiToken) {
        this.group = group;
        this.namespace = namespace;
        this.topic = topic;
        this.apiUri = apiUri;
        this.apiToken = apiToken;
    }

    public void addClient(final String clientId) {
        if (this.getClients() == null) {
            this.setClients(new ArrayList<>());
        }
        this.getClients().add(clientId);
    }

}
