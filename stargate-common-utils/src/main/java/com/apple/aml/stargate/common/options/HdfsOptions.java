package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.STARGATE;
import static com.apple.aml.stargate.common.utils.AppConfig.mode;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.util.Arrays.asList;

@Data
@EqualsAndHashCode(callSuper = true)
public class HdfsOptions extends FileOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String coreXmlFileName;
    @Optional
    private String hdfsXmlFileName;
    @Optional
    private String hiveXmlFileName;
    @Optional
    private String krb5FileName;
    @Optional
    private String keytabFileName;
    @Optional
    private Map<String, Object> coreProperties;
    @Optional
    private Map<String, Object> hdfsProperties;
    @Optional
    private Map<String, Object> hiveProperties;
    @Optional
    private Map<String, Object> sparkProperties;
    @Optional
    private String krb5;
    @Optional
    private String principal;
    @Optional
    private String defaultFsName;
    @Optional
    private boolean autoUpdateTable;
    @Optional
    private String dbName;
    @Optional
    private String dbPath;
    @Optional
    private String tableName;

    public String keytabFileName() {
        if (isBlank(this.keytabFileName)) {
            return "keytab";
        }
        return this.keytabFileName;
    }

    public String krb5FileName() {
        if (isBlank(this.krb5FileName)) {
            return "krb5";
        }
        return this.krb5FileName;
    }

    @Override
    public String tmpPath(final String pipelineId) {
        return hdfsTmpPath(this.getTmpPath(), this.getBasePath(), defaultFsName, principal, pipelineId);
    }

    public static String hdfsTmpPath(final String tmpPath, final String basePath, final String defaultFsName, final String principal, final String pipelineId) {
        if (isBlank(tmpPath)) {
            return defaultFsName + "/user/" + principal + "/" + asList(STARGATE, mode().toLowerCase() + "-pipelines", pipelineId, System.getProperty("user.name") + "-tmp").stream().collect(Collectors.joining("/"));
        }
        if (tmpPath.contains("://")) {
            return tmpPath;
        }
        if (tmpPath.startsWith("/")) {
            return defaultFsName + tmpPath;
        } else {
            return defaultFsName + "/user/" + principal + "/" + tmpPath;
        }
    }

    @Override
    public String basePath() {
        return hdfsBasePath(this.getBasePath(), defaultFsName, principal);
    }

    public static String hdfsBasePath(final String basePath, final String defaultFsName, final String principal) {
        if (isBlank(basePath)) {
            return defaultFsName + "/user/" + principal;
        }
        if (basePath.contains("://")) {
            return basePath;
        }
        if (basePath.startsWith("/")) {
            return defaultFsName + basePath;
        } else {
            return defaultFsName + "/user/" + principal + "/" + basePath;
        }
    }

}
