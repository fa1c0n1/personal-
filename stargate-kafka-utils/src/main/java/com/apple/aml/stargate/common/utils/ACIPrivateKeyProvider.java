package com.apple.aml.stargate.common.utils;

import com.apple.pie.queue.common.crypto.AsymmetricKeyCrypto;
import com.apple.pie.queue.kafka.client.kaffe.privatekeyprovider.PrivateKeyProvider;
import com.apple.pie.queue.kafka.client.kaffe.privatekeyprovider.PrivateKeyProviderException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.PRIVATE_KEY_HEADER_INCLUDES;
import static com.apple.aml.stargate.common.constants.KafkaConstants.KAFFE_CONFIG_PREFIX;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static java.lang.Boolean.parseBoolean;

public class ACIPrivateKeyProvider implements PrivateKeyProvider {
    private static final String PROP_ACI_CLIENT_KEY = configPrefix() + ".aci.kaffe.client.secret.private.key";
    private static final String PROP_ACI_CLIENT_ENCODING = configPrefix() + ".aci.kaffe.client.private.key.encoded";
    private String privateKey;

    @SuppressWarnings("unchecked")
    public static <V> Map<String, V> setPrivateKeyProvider(final Map<String, V> configs, final String privateKey) {
        setPrivateKey(configs, privateKey);
        configs.put(KAFFE_CONFIG_PREFIX + ".private.key.provider", (V) ACIPrivateKeyProvider.class);
        return configs;
    }

    @SuppressWarnings("unchecked")
    public static <V> Map<String, V> setPrivateKey(final Map<String, V> configs, final String privateKey) {
        configs.put(PROP_ACI_CLIENT_KEY, (V) privateKey);
        return configs;
    }

    @Override
    public AsymmetricKeyCrypto providePrivateKey() {
        try {
            return AsymmetricKeyCrypto.ofPrivateKey(privateKey);
        } catch (Exception e) {
            throw new PrivateKeyProviderException("Could not decode ACI Kaffe privateKey value", e);
        }
    }

    @Override
    public void close() throws RuntimeException {

    }

    @Override
    public void configure(final Map<String, ?> configs) {
        try {
            privateKey = (String) configs.get(PROP_ACI_CLIENT_KEY);
            if (!privateKey.contains(PRIVATE_KEY_HEADER_INCLUDES)) {
                String isEncoded = (String) configs.get(PROP_ACI_CLIENT_ENCODING);
                if (isEncoded == null || parseBoolean(isEncoded)) {
                    privateKey = new String(Base64.getDecoder().decode(privateKey), StandardCharsets.UTF_8);
                }
            }
        } catch (Exception e) {
            throw new PrivateKeyProviderException("Could not find required ACI Kaffe Client Key", e);
        }
    }
}
