package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.constants.CommonConstants;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class SolrOptions extends SolrQueryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String url;
    @Optional
    private String zkHost;
    @Optional
    private String user;
    @Optional
    private String password;
    @Optional
    private boolean useBasicAuth = true;
    @Optional
    private Duration connectionTimeout;
    @Optional
    private Duration socketTimeout;
    @Optional
    private String operation = "read";
    @Optional
    private boolean dynamic;
    @Optional
    private boolean useInbuilt;
    @Optional
    private String timestampFieldName;
    @Optional
    private boolean useLatestTimestamp = true;
    @Optional
    private String collectionType;
    @Optional
    private String timestampUnit;
    @Optional
    private boolean bounded = true;
    @Optional
    private int boundedSplitSize = 500;
    @Optional
    private boolean useTimestampStats = true;
    @Optional
    private List<SolrQueryOptions> options;
    @Optional
    private Duration queryRetryDuration = Duration.ofSeconds(15);
    @Optional
    private int queryRetryCount = 5;
    @Optional
    private String readFrom;
    @Optional
    private Duration pollDuration = Duration.ofSeconds(60);
    @Optional
    private Duration pollRetryDuration = Duration.ofSeconds(120);
    @Optional
    private String deleteAttributeName = CommonConstants.OfsConstants.DELETE_ATTRIBUTE_NAME;
    @Optional
    private String solrIdField = CommonConstants.ID;
    @Optional
    private int batchEndNumChecks = 3;
    @Optional
    private int maxConnections = 20;
    @Optional
    private int maxConnectionPerRoute = 4;
    @Optional
    private List<String> zookeeperHostList;
    @Optional
    private boolean useZookeeper;
    @Optional
    private boolean kerberizationEnabled;
    @Optional
    private boolean useHardCommit;
    @Optional
    private String krb5FileName;
    @Optional
    private String keytabFileName;
    @Optional
    private String jaasFileName;
    @Optional
    private String zkChroot = "/solr";
    @Optional
    private String krbPrincipal;
    @Optional
    private String krb5;
    @Optional
    private boolean useKrb5AuthDebug;

    public void sanitizeAuthOptions() {
        useBasicAuth = useBasicAuth && !isBlank(this.user) && !isBlank(this.password);
        if (useBasicAuth) {
            kerberizationEnabled = false;
        }
    }

    public String keytabFileName() {
        if (isBlank(this.keytabFileName)) {
            return "solr";
        }
        return this.keytabFileName;
    }

    public String krb5FileName() {
        if (isBlank(this.krb5FileName)) {
            return "krb5";
        }
        return this.krb5FileName;
    }

    public String jaasFileName() {
        if (isBlank(this.jaasFileName)) {
            return "jaas-client";
        }
        return this.jaasFileName;
    }
}
