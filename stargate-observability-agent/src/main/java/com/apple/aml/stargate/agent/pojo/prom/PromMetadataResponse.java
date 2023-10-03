package com.apple.aml.stargate.agent.pojo.prom;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@lombok.Data
@ToString
public class PromMetadataResponse implements Serializable {

    private String status;

    @JsonProperty("data")
    private List<PromMetadata> data;
}
