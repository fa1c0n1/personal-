package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;

@Data
public class StageSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    @BsonProperty(value = "name")
    private String name;

    @BsonProperty(value = "count")
    private long count;

    @BsonProperty(value = "rate")
    private long rate;

    @BsonProperty(value = "total")
    private long total;

    @BsonProperty(value = "p99Time")
    private double p99Time;

}
