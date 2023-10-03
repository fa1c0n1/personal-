package com.apple.aml.stargate.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorPayload implements Serializable {
    private long appId;
    private String pipelineId;
    private String nodeName;
    private String nodeType;
    private String stage;
    private String key;
    private String payload; // base64 encoded byte string of AVRO GenericRecord
    private String schemaId;
    private int schemaVersion;
    private String json;
    private String errorClass;
    private String errorMessage;
    private String errorStackTrace;
    private Map<String, String> errorInfo;
}
