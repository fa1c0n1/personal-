package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.util.Arrays.asList;

@Data
@EqualsAndHashCode(callSuper = true)
public class JdbcOptions extends SchemaMappingOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String user;
    @Optional
    private String username;
    @Optional
    private String userName;
    @Optional
    private String password;
    @Optional
    private String connectionString;
    @Optional
    private Map<String, Object> datasourceProperties;
    @Optional
    private String schema;
    @Optional
    private String database;
    @Optional
    private String driver;
    @Optional
    private String role;
    @Optional
    private String operation = "write";
    @Optional
    private String expression;
    @Optional
    private String keyAttribute; // set it to primaryKey attribute if it exists; for now composite keys are not supported
    @Optional
    private String columnNameCaseFormat;
    @Optional
    private int successStatus;

    public String userName() {
        java.util.Optional<String> optional = asList(user, username, userName).stream().filter(x -> !isBlank(x)).findFirst();
        return optional.isPresent() ? optional.get() : null;
    }

    public String driverName() {
        if (!isBlank(driver)) return this.driver;
        if (connectionString.contains("mysql://")) {
            return "com.mysql.jdbc.Driver";
        }
        return null; // TODO : Add support for auto-detect other drivers
    }
}
