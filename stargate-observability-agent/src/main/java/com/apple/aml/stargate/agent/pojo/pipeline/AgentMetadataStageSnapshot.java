package com.apple.aml.stargate.agent.pojo.pipeline;

import lombok.Data;

import java.io.Serializable;

@Data
public class AgentMetadataStageSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private long count;

    private long rate;

    private long total;

    private double p99Time;

}
