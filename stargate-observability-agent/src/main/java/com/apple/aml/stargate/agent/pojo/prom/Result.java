package com.apple.aml.stargate.agent.pojo.prom;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@ToString
public class Result implements Serializable {
    private Metric metric;
    private ArrayList<Object> values;
}

