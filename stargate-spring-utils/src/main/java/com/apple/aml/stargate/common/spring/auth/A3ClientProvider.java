package com.apple.aml.stargate.common.spring.auth;

import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.appeng.aluminum.auth.idms.apptoapp.AppToAppTokenClientProvider;
import com.apple.ist.idms.i3.a3client.pub.A3TokenClientI;

public class A3ClientProvider implements AppToAppTokenClientProvider {
    @Override
    public A3TokenClientI getA3TokenClient() {
        return A3Utils.client();
    }
}
