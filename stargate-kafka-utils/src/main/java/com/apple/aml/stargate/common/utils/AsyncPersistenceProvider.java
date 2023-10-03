package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.pojo.AsyncMetadata;
import com.apple.aml.stargate.common.pojo.AsyncPayload;
import io.prometheus.client.Histogram;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.apple.aml.stargate.common.constants.CommonConstants.IDMS_APP_ID;
import static com.apple.aml.stargate.common.constants.KafkaConstants.PUBLISHER_ID;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;

public interface AsyncPersistenceProvider {

    String ROCKSDB = "ROCKSDB";
    String PAYLOAD_UUID = "PAYLOAD_UUID";
    Histogram HISTOGRAM_DURATION = bucketBuilder("async_db_duration", Histogram.build().help("Time taken various methods in milli seconds.").labelNames(IDMS_APP_ID, PUBLISHER_ID, "methodName")).register();
    Histogram HISTOGRAM_SIZE = bucketBuilder("async_db_byte_length", Histogram.build().help("Raw byte array length").labelNames(IDMS_APP_ID, PUBLISHER_ID, "methodName")).register();

    void initializeDb(final String dbpath) throws Exception;

    boolean ping() throws Exception;

    void persistPayload(final AsyncPayload payload) throws Exception;

    boolean updateMetadata(final AsyncMetadata metadata) throws Exception;

    byte[] retrievePayload(final byte[] uuid) throws Exception;

    AsyncPayload retrieveAsyncPayload(final long appId, final String publisherId, final byte[] uuid) throws Exception;

    void removePayload(final long appId, final String publisherId, final byte[] uuid) throws Exception;

    void removeMetadata(final long appId, final String publisherId, final byte[] uuid) throws Exception;

    void removePayload(final byte[] uuid) throws Exception;

    void processPayload(final Consumer<Pair<byte[], byte[]>> consumer) throws Exception;

    AsyncMetadata deserializeMetadata(byte[] bytes) throws Exception;

    Map<String, Map<String, Number>> getDbStats() throws Exception;

    Map<String, Object> providerInfo() throws Exception;

    List<String> retrieveLoadedPublisherIds() throws Exception;

    List<String> retrieveLivePayloadKeys(final long appId, final String publisherId) throws Exception;

    void gracefulShutDown();

    static AsyncPersistenceProvider getKafkaPersistenceProvider(final Consumer<Pair<byte[], byte[]>> consumer) throws Exception {
        String persistenceProviderKey = String.format("%s.async.persistence.db.type", configPrefix());
        String persistenceProvider = config().hasPath(persistenceProviderKey) ? config().getString(persistenceProviderKey) : ROCKSDB;
        if (!ROCKSDB.equalsIgnoreCase(persistenceProvider)) throw new UnsupportedOperationException();
        String basePath = config().getString(String.format("%s.async.persistence.dir.path", configPrefix()));
        String deletePathOnStartupKey = String.format("%s.async.persistence.dir.deleteOnStartup", configPrefix());
        if (config().hasPath(deletePathOnStartupKey) && config().getBoolean(deletePathOnStartupKey)) FileUtils.deleteDirectory(new File(basePath));
        return RocksDBAsyncPersistenceProvider.getInstance(basePath, config().getInt(String.format("%s.async.event.retry.poll.duration", configPrefix())), consumer);
    }

}
