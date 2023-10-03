package com.apple.aml.stargate.common.pojo;

import com.apple.ist.idms.i3.a3client.pub.A3TokenInfo;

import static com.apple.jvm.commons.util.Strings.isBlank;

public class A3Token extends A3TokenInfo {

    private String appAdminPassword;

    public A3Token() {
        super(null, null, null);
    }

    public A3Token(final Long appId, final Long otherAppId, final String context) {
        super(appId, otherAppId, context);
    }

    public A3Token(final Long appId, final Long otherAppId, final String context, final String region, final String otherRegion) {
        super(appId, otherAppId, context, region, otherRegion);
    }

    public String getAppAdminPassword() {
        return appAdminPassword;
    }

    public void setAppAdminPassword(final String appAdminPassword) {
        this.appAdminPassword = appAdminPassword;
    }

    public String appPassword(final String defaultValue) {
        if (isBlank(this.appAdminPassword)) {
            return defaultValue;
        }
        return this.appAdminPassword;
    }
}
