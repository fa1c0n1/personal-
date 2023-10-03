package com.apple.aml.stargate.pipeline.sdk.io.hadoop;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class HdfsSharedConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String SYS_PROP_KRB5_CONF = "java.security.krb5.conf";
    private String principal;
    private String keytabFileName;
    private String krb5FileName;
    private String coreXmlFileName;
    private String hdfsXmlFileName;
    private String hiveXmlFileName;
    private Map<String, String> config;
}
