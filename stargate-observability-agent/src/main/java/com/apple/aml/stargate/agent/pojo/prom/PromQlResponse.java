package com.apple.aml.stargate.agent.pojo.prom;

import lombok.ToString;

import java.io.Serializable;

@lombok.Data
@ToString
public class PromQlResponse implements Serializable {
    private String status;
    private Data data;
}

