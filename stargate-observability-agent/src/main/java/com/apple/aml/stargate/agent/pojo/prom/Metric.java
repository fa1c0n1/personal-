package com.apple.aml.stargate.agent.pojo.prom;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class Metric implements Serializable {
    private String __name__;
    private String app_kubernetes_io_name;
    private String component;
    private String instance;
    private String job;
    private String kubernetes_namespace;
    private String kubernetes_pod_name;
    private String node;
    private String nodeName;
    private String pipelineId;
    private String platform;
    private String runNo;
    private String le;
    private String stage;
    private String schemaId;
    private String workflow;
    private String type;

    private String attributeName;

    private String attributeValue;

    public Metric() {
    }

    public Metric(final Metric metric) {
        this.__name__ = metric.__name__;
        this.app_kubernetes_io_name = metric.app_kubernetes_io_name;
        this.component = metric.component;
        this.instance = metric.instance;
        this.job = metric.job;
        this.kubernetes_namespace = metric.kubernetes_namespace;
        this.kubernetes_pod_name = metric.kubernetes_pod_name;
        this.node = metric.node;
        this.nodeName = metric.nodeName;
        this.pipelineId = metric.pipelineId;
        this.platform = metric.platform;
        this.runNo = metric.runNo;
        this.le = metric.le;
        this.stage = metric.stage;
        this.schemaId = metric.schemaId;
        this.workflow = metric.workflow;
        this.type = metric.type;
        this.attributeName = metric.attributeName;
        this.attributeValue = metric.attributeValue;
    }
}
