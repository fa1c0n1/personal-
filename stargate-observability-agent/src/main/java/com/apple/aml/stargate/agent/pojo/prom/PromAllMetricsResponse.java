package com.apple.aml.stargate.agent.pojo.prom;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@ToString
public class PromAllMetricsResponse implements Serializable {

    public ArrayList<String> data;
}
