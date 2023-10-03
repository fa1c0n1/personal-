package com.apple.aml.stargate.agent.pojo.prom;

import lombok.ToString;

import java.io.Serializable;

@lombok.Data
@ToString
public class PromMetadata implements Serializable {

    private String metric;

    private String type;

}
