package com.apple.pie.queue.kafka.crypto;

import com.apple.pie.crypto.kafka.ConsumerKafkaCrypt;
import com.apple.pie.crypto.kafka.ConsumerKafkaCryptIrisUtil;
import com.apple.pie.crypto.kafka.exceptions.ConsumerNotFoundException;
import com.apple.pie.crypto.kafka.exceptions.InternalException;
import com.apple.pie.crypto.kafka.exceptions.TopicNotFoundException;
import com.apple.pie.crypto.kafka.exceptions.UnauthorizedException;
import com.apple.pie.crypto.kafka.util.CryptoClientConfig;
import com.apple.pie.crypto.kafka.utils.ConsumerCryptoConfig;
import com.apple.pie.queue.kafka.crypto.extension.CryptoExtensionKey;
import com.apple.pie.queue.kafka.envelope.ExtensionAwareDeserializer;
import com.apple.pie.queue.kafka.envelope.extensions.EnvelopeExtension;
import com.apple.pie.queue.kafka.envelope.extensions.ExtensionKey;
import com.apple.pie.queue.kafka.envelope.internal.ByteBufferOrValue;
import com.apple.pie.queue.kafka.envelope.internal.SerDeUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.apple.aml.stargate.common.utils.PrometheusUtils.metricsService;
import static com.apple.pie.queue.kafka.crypto.EncryptionSerializerConfigDef.PAYLOAD_SERIALIZER_CONFIG;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DecryptionDeserializer<T> implements ExtensionAwareDeserializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DecryptionDeserializer.class);
    private static final String CONSUMER_CRYPTO_CLIENT_METRICS_PREFIX = "consumer.crypto.client - ";

    @Nullable
    @SuppressWarnings("WeakerAccess")
    protected ConsumerKafkaCrypt decrypter;
    private ExtensionAwareDeserializer<T> payloadDeserializer;

    /**
     * Use the constructor that takes a payloadDeserializer and configs instead.
     */
    @Deprecated
    public DecryptionDeserializer() {
    }

    /**
     * Construct a DecryptionSerializer using the provided raw config and payloadDeserializer.
     */
    public DecryptionDeserializer(Deserializer<T> payloadDeserializer, Map<String, ?> configs) {
        final DecryptionDeserializerConfigDef config = DecryptionDeserializerConfigDef.of(configs, true);
        setup(SerDeUtils.toExtensionAwareDeserializer(payloadDeserializer), config, configs);
    }

    private void setup(ExtensionAwareDeserializer<T> payloadDeserializer, DecryptionDeserializerConfigDef config, Map<String, ?> configs) {
        this.payloadDeserializer = requireNonNull(payloadDeserializer, "payloadDeserializer needs to be set");
        if (!config.getBoolean(DecryptionDeserializerConfigDef.TRY_TO_DECRYPT_MESSAGES_CONFIG)) {
            throw new IllegalArgumentException(format("You need to set %s to use the decryption deserializer", DecryptionDeserializerConfigDef.TRY_TO_DECRYPT_MESSAGES_CONFIG));
        }
        setupDecrypter(config);
    }

    protected void setupDecrypter(DecryptionDeserializerConfigDef configDef) {
        try {
            ConsumerCryptoConfig consumerCryptoConfig = new ConsumerCryptoConfig.Builder().withConsumer(configDef.getString(DecryptionDeserializerConfigDef.CONSUMER_ID_KEY_CONFIG), configDef.getList(KafkaCryptoConfig.PREFETCH_KEYS_FOR_TOPICS_CONFIG)).withMetricReporter(metricsService(), CONSUMER_CRYPTO_CLIENT_METRICS_PREFIX).setBlockingFutureTimeout(configDef.getInt(KafkaCryptoConfig.IRIS_CLIENT_TIMEOUT_MILLIS_CONFIG)).build();
            CryptoClientConfig.Builder cryptoClientConfigBuilder = new CryptoClientConfig.Builder().setConsumerPrivateKey(configDef.getPassword(DecryptionDeserializerConfigDef.CONSUMER_SECRET_KEY_CONFIG).value());
            CryptoClientConfig cryptoClientConfig = KafkaCryptoConfig.decorateCryptoClientConfigBuilderWithCommonConfigs(cryptoClientConfigBuilder, configDef).build();
            LOGGER.info("Initializing crypto deserializer.");
            decrypter = ConsumerKafkaCryptIrisUtil.from(consumerCryptoConfig, cryptoClientConfig);
            LOGGER.info("Successfully initialized crypto deserializer.");
        } catch (InternalException | UnauthorizedException | ConsumerNotFoundException | TopicNotFoundException ex) {
            LOGGER.error("Error initializing crypto deserializer");
            throw new RuntimeException("Error initializing crypto client with given configs.", ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        if (isKey) {
            throw new IllegalArgumentException("Can't use decryption deserializer as a key serializer.");
        }
        final DecryptionDeserializerConfigDef config = DecryptionDeserializerConfigDef.of(configs, true);
        Class<?> aClass = config.getClass(PAYLOAD_SERIALIZER_CONFIG);
        if (aClass.isAssignableFrom(String.class)) {
            // This is the value being set up in EncryptionSerializerConfigDef to support both
            // live instances through the constructor and configuring instances with configure()
            throw new ConfigException(format("Config '%s' needs to be set", PAYLOAD_SERIALIZER_CONFIG));
        }
        ExtensionAwareDeserializer<T> payloadDeserializer = requireNonNull(SerDeUtils.toExtensionAwareDeserializer(config.getConfiguredInstance(DecryptionDeserializerConfigDef.PAYLOAD_DESERIALIZER_CONFIG, Deserializer.class)));
        payloadDeserializer.configure(configs, false);
        setup(payloadDeserializer, config, configs);
    }

    @Override
    public void close() {
        LOGGER.info("Closing crypto deserializer.");
        if (decrypter != null) {
            LOGGER.info("Closing decrypter client");
            decrypter.close();
        }
        if (payloadDeserializer != null) {
            payloadDeserializer.close();
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data, Map<ExtensionKey, ByteBufferOrValue<EnvelopeExtension>> extensions) {
        if (data == null) {
            return null;
        }

        byte[] decryptedBytes = data;
        if (extensions.containsKey(CryptoExtensionKey.CRYPTO_EXTENSION_KEY) || headers.lastHeader(CryptoExtensionKey.CRYPTO_HEADER_NAME) != null) {
            decryptedBytes = decryptPayload(topic, data);
        }
        return deserializePayload(topic, headers, decryptedBytes, extensions);
    }

    private byte[] decryptPayload(final String topic, final byte[] data) {
        try {
            LOGGER.trace("Decrypting message for topic={}.", topic);
            //noinspection ConstantConditions
            byte[] decryptedBytes = decrypter.decrypt(topic, data);
            return decryptedBytes;
        } catch (ConsumerNotFoundException | InternalException | TopicNotFoundException | UnauthorizedException ex) {
            LOGGER.error("Failed to decrypt.", ex);
            throw new SerializationException("Failed to decrypt.", ex);
        }
    }

    private T deserializePayload(final String topic, final Headers headers, final byte[] decryptedBytes, final Map<ExtensionKey, ByteBufferOrValue<EnvelopeExtension>> extensions) {
        T deserializedBytes = payloadDeserializer.deserialize(topic, headers, decryptedBytes, extensions);
        return deserializedBytes;
    }

}

