package com.apple.aml.stargate.agent.pojo.prom;

import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;

@lombok.Data
@ToString
public class Data implements Serializable {
    private String resultType;
    private ArrayList<Result> result;
}

