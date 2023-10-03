package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class SnowflakeOptions extends JdbiOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String account;
    @Optional
    private String privateKey;
    @Optional
    private String passphrase;
    @Optional
    private String warehouse;

    @Override
    public String getConnectionString() {
        String url = super.getConnectionString();
        if (isBlank(url)) return String.format("jdbc:snowflake://%s.snowflakecomputing.com", getAccount());
        return url;
    }

    @Override
    public String driverName() {
        if (isBlank(super.getDriver())) return "net.snowflake.client.jdbc.SnowflakeDriver";
        return super.driverName();
    }
}
