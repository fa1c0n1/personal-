package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.annotation.DesignerSpec;
import com.apple.aml.stargate.common.annotation.DesignerSpec.Type;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class ACICassandraOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    @DesignerSpec(label = "casseroleURL", toolTip = "casserole Url", type = Type.SingleLineText)
    private String casseroleURL;
    @Optional
    @DesignerSpec(label = "password", toolTip = "Cassandra Password", type = Type.SingleLineText)
    private String password;
    @Optional
    @DesignerSpec(label = "clusterName", toolTip = "Cluster Name", type = Type.SingleLineText)
    private String clusterName;
    @Optional
    @DesignerSpec(label = "userName", toolTip = "User Name", type = Type.SingleLineText)
    private String userName;
    @Optional
    @DesignerSpec(label = "truststorePassword", toolTip = "Truststore Password", type = Type.SingleLineText)
    private String truststorePassword;
    @Optional
    @DesignerSpec(label = "truststoreLocation", toolTip = "Truststore Location", type = Type.SingleLineText)
    private String truststoreLocation;
    @Optional
    @DesignerSpec(label = "dc", toolTip = "DC", type = Type.SingleLineText)
    private String dc;
    @Optional
    @DesignerSpec(label = "port", toolTip = "Port", type = Type.Number)
    private Integer port;
    @Optional
    @DesignerSpec(label = "Parallel Processing", toolTip = "Parallel Processing", type = Type.YesNo)
    private boolean parallel = true;
    @Optional
    @DesignerSpec(label = "keySpaceName", toolTip = "Cassandra Keyspace", type = Type.SingleLineText)
    private String keySpaceName;
    @Optional
    @DesignerSpec(label = "tableName", toolTip = "Cassandra table name", type = Type.SingleLineText)
    private String tableName;
    @Optional
    @DesignerSpec(label = "recordIdentifier", toolTip = "Cassandra table unique Id field", type = Type.SingleLineText)
    private String recordIdentifier;
    @Optional
    @DesignerSpec(label = "primaryKeys", toolTip = "list all Partition keys and Clustering keys", type = Type.List)
    private List<String> primaryKeys;
    @Optional
    @DesignerSpec(label = "hosts", toolTip = "Hosts for Cassandra Connection", type = Type.List)
    private List<String> hosts;
    @Optional
    @DesignerSpec(label = "applicationName", toolTip = "Application name in ACI Cassandra ", type = Type.SingleLineText)
    private String applicationName;
    @Optional
    @DesignerSpec(label = "Query", toolTip = "User Defined Query", type = Type.SingleLineText)
    private String query;
    @Optional
    @DesignerSpec(label = "Vanilla", toolTip = "Vanilla", type = Type.YesNo)
    private boolean vanilla;
    @Optional
    @DesignerSpec(label = "ThrowOnError", toolTip = "Throw On Error", type = Type.YesNo)
    private boolean throwOnError = true; // TODO : Need to get rid of this
    @Optional
    @DesignerSpec(label = "LogOnError", toolTip = "LOG On Error", type = Type.YesNo)
    private boolean logOnError; // TODO : Need to get rid of this
    @Optional
    @DesignerSpec(label = "MinimumNumberOfSplits", toolTip = "Minimum number of splits", type = Type.Number)
    private int minNumberOfSplits;
    @Optional
    @DesignerSpec(label = "ConnectTimeout", toolTip = "Connect Timeout", type = Type.Number)
    private int connectTimeout;
    @Optional
    @DesignerSpec(label = "ReadTimeout", toolTip = "Read Timeout", type = Type.Number)
    private int readTimeout;
    @Optional
    @DesignerSpec(label = "ConsistencyLevel", toolTip = "Consistency level", type = Type.SingleChoice, choices = {"ONE", "TWO", "THREE"})
    private String consistencyLevel;
}
