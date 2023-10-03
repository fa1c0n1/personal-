package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.pojo.AsyncMetadata;
import com.apple.aml.stargate.common.pojo.AsyncPayload;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.NOT_APPLICABLE_HYPHEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.STARGATE;
import static com.apple.aml.stargate.common.pojo.AsyncMetadata.bytesToUUID;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.ClassUtils.updateObject;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.NetworkUtils.hostName;
import static com.apple.aml.stargate.common.utils.ThreadUtils.threadFactory;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Long.parseLong;

public class RocksDBAsyncPersistenceProvider implements AsyncPersistenceProvider {
    protected static final Logger LOGGER = logger(AsyncPersistenceProvider.class);
    private static final boolean isTraceEnabled = LOGGER.isTraceEnabled();
    private static final String COLUMN_FAMILY_NAME_DEFAULT = "default";
    private static final String COLUMN_FAMILY_NAME_PAYLOAD = "payload";
    private static final FSTConfiguration FST_CONF = fstConfiguration();
    private static final String dbKeyDelimiter = "~~";
    private static final String compactOption = String.format("%s.async.persistence.rocksdb.autocompact", configPrefix());
    private static final String payloadCompressionOption = String.format("%s.async.persistence.rocksdb.payload.compression", configPrefix());
    private static final byte[] pingKey = STARGATE.getBytes(StandardCharsets.UTF_8);
    private static final byte[] pingValue = STARGATE.getBytes(StandardCharsets.UTF_8);
    private final Map<String, Object> optionOverrides = new HashMap<>();
    private Map<String, Object> providerInfo = new HashMap<>();
    private RocksDB store = null;
    private WriteOptions writeOptions = null;
    private ColumnFamilyHandle defaultColumnFamilyHandle = null;
    private ColumnFamilyHandle payloadColumnFamilyHandle = null;
    private final ConcurrentHashMap<String, ColumnFamilyHandle> columnFamilyMap = new ConcurrentHashMap<>();
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService pollService = Executors.newSingleThreadScheduledExecutor(threadFactory(String.format("async-process-rocksdb-thread-%d", System.currentTimeMillis()), true));
    private static final Cache<UUID, Long> METADATA_CACHE = new Cache2kBuilder<UUID, Long>() {
    }.expireAfterWrite((config().hasPath(String.format("%s.async.persistence.metadata.cache.duration", configPrefix())) ? config().getInt(String.format("%s.async.persistence.metadata.cache.duration", configPrefix())) : 60), TimeUnit.SECONDS).build();

    @SuppressWarnings("unchecked")
    protected RocksDBAsyncPersistenceProvider() {
        RocksDB.loadLibrary();
        if (config().hasPath(String.format("%s.async.persistence.rocksdb.options", configPrefix()))) {
            Map options = readJsonMap(config().getString(String.format("%s.async.persistence.rocksdb.options", configPrefix())));
            if (!options.isEmpty()) optionOverrides.putAll(options);
        }
    }

    private static FSTConfiguration fstConfiguration() {
        FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
        conf.registerClass(AsyncMetadata.class, AsyncPayload.class, LinkedHashSet.class, HashMap.class, byte[].class);
        return conf;
    }

    private static byte[] toBytes(final AsyncMetadata metadata) {
        return FST_CONF.asByteArray(metadata);
    }

    public static AsyncMetadata toPojo(byte[] bytes) {
        return (AsyncMetadata) FST_CONF.asObject(bytes);
    }

    public static RocksDBAsyncPersistenceProvider getInstance(final String persistencePath, final int pollDuration, final Consumer<Pair<byte[], byte[]>> consumer) {
        RocksDBAsyncPersistenceProvider instance = new RocksDBAsyncPersistenceProvider();
        try {
            instance.initializeDb(persistencePath);
            instance.pollService.scheduleWithFixedDelay(() -> {
                try {
                    instance.processPayload(consumer);
                } catch (Exception e) {
                    LOGGER.error("Error in processing pending keys", Map.of(ERROR_MESSAGE, e.getMessage()), e);
                }
            }, pollDuration, pollDuration, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.error("Error in initializing RocksDB", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new IllegalStateException(e);
        }
        return instance;
    }

    @Override
    public void initializeDb(final String baseDbpath) throws Exception {
        new File(baseDbpath).mkdirs();
        getOrCreateDB(baseDbpath, 0);
    }

    @Override
    public boolean ping() throws Exception {
        return _ping(store);
    }

    private boolean _ping(final RocksDB db) {
        try {
            db.put(pingKey, pingValue);
            db.delete(pingKey);
        } catch (RocksDBException ex) {
            LOGGER.error("RocksDB Not available", Map.of("dbHost", hostName()), ex);
            return false;
        }
        return true;
    }

    @Override
    public void persistPayload(final AsyncPayload payload) throws RocksDBException {
        long startTime = System.nanoTime();
        AsyncMetadata metadata = payload.getMetadata();
        String metricAppId = String.valueOf(metadata.getMetricAppId());
        String publisherId = String.valueOf(metadata.getPublisherId());
        try {
            byte[] key = metadata.getUuid();
            byte[] bytes = payload.getPayload();
            store.put(payloadColumnFamilyHandle, writeOptions, key, bytes);
            HISTOGRAM_SIZE.labels(metricAppId, publisherId, "persistPayload::payload").observe(bytes.length);
            bytes = toBytes(metadata);
            ColumnFamilyHandle handle = getPublisherColumnFamily(metadata);
            store.put(handle, writeOptions, key, bytes);
            HISTOGRAM_SIZE.labels(metricAppId, publisherId, "persistPayload::metadata").observe(bytes.length);
            METADATA_CACHE.put(metadata.UUID(), System.currentTimeMillis());
        } finally {
            HISTOGRAM_DURATION.labels(metricAppId, publisherId, "persistPayload").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @Override
    public boolean updateMetadata(final AsyncMetadata metadata) throws Exception {
        long startTime = System.nanoTime();
        String metricAppId = String.valueOf(metadata.getMetricAppId());
        String publisherId = String.valueOf(metadata.getPublisherId());
        try {
            byte[] key = metadata.getUuid();
            UUID uuid = metadata.UUID();
            synchronized (metadata.getUuid()) {
                if (METADATA_CACHE.containsKey(uuid)) {
                    ColumnFamilyHandle handle = getPublisherColumnFamily(metadata);
                    store.put(handle, writeOptions, key, toBytes(metadata));
                    return true;
                } else {
                    ColumnFamilyHandle handle = getPublisherColumnFamily(metadata);
                    try {
                        byte[] existing = store.get(handle, key);
                        if (existing == null || existing.length == 0) return false;
                        METADATA_CACHE.put(uuid, System.currentTimeMillis());
                        store.put(handle, writeOptions, key, toBytes(metadata));
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                }
            }
        } finally {
            HISTOGRAM_DURATION.labels(metricAppId, publisherId, "updateMetadata").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @Override
    public byte[] retrievePayload(final byte[] uuid) throws Exception {
        long startTime = System.nanoTime();
        try {
            return store.get(payloadColumnFamilyHandle, uuid);
        } finally {
            HISTOGRAM_DURATION.labels(NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN, "retrievePayload").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    private ColumnFamilyHandle getPublisherColumnFamily(final AsyncMetadata metadata) {
        final String columnFamilyName = String.format("%d%s%s", metadata.getAppId(), dbKeyDelimiter, metadata.getPublisherId());
        return columnFamilyMap.computeIfAbsent(columnFamilyName, k -> createColumnFamily(columnFamilyName));
    }

    @SneakyThrows
    private ColumnFamilyHandle createColumnFamily(final String columnFamilyName) {
        ColumnFamilyHandle handle = store.createColumnFamily(new ColumnFamilyDescriptor(columnFamilyName.getBytes(StandardCharsets.UTF_8), getColumnFamilyOptions()));
        if (config().hasPath(compactOption) && parseBoolean(config().getString(compactOption))) {
            List<ColumnFamilyHandle> handles = new ArrayList<>(columnFamilyMap.values());
            if (defaultColumnFamilyHandle != null) handles.add(defaultColumnFamilyHandle);
            handles.add(payloadColumnFamilyHandle);
            handles.add(handle);
            store.enableAutoCompaction(handles);
        }
        return handle;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private ColumnFamilyOptions getColumnFamilyOptions() {
        ColumnFamilyOptions options = new ColumnFamilyOptions();
        Map cfOverrides = (Map) optionOverrides.get("columnFamilyOptions");
        if (cfOverrides == null || cfOverrides.isEmpty()) return options;
        return updateObject(options, cfOverrides);
    }

    @Override
    public AsyncPayload retrieveAsyncPayload(final long appId, final String publisherId, final byte[] uuid) throws Exception {
        long startTime = System.nanoTime();
        try {
            String columnFamilyKey = String.format("%d%s%s", appId, dbKeyDelimiter, publisherId);
            ColumnFamilyHandle handle = columnFamilyMap.get(columnFamilyKey);
            if (handle == null) return null;
            byte[] rawValue = store.get(handle, uuid);
            if (rawValue == null) return null;
            return AsyncPayload.builder().metadata(toPojo(rawValue)).payload(store.get(payloadColumnFamilyHandle, uuid)).build();
        } finally {
            HISTOGRAM_DURATION.labels(String.valueOf(appId), String.valueOf(publisherId), "retrieveAsyncPayload").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @Override
    public void removePayload(final long appId, final String publisherId, final byte[] uuid) throws Exception {
        long startTime = System.nanoTime();
        try {
            METADATA_CACHE.remove(bytesToUUID(uuid));
            store.delete(payloadColumnFamilyHandle, uuid);
            String columnFamilyKey = String.format("%d%s%s", appId, dbKeyDelimiter, publisherId);
            ColumnFamilyHandle handle = columnFamilyMap.get(columnFamilyKey);
            if (handle == null) return;
            store.delete(handle, uuid);
        } finally {
            HISTOGRAM_DURATION.labels(String.valueOf(appId), String.valueOf(publisherId), "removePayload::detailed").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @Override
    public void removePayload(final byte[] uuid) throws Exception {
        long startTime = System.nanoTime();
        try {
            METADATA_CACHE.remove(bytesToUUID(uuid));
            store.delete(payloadColumnFamilyHandle, uuid);
        } finally {
            HISTOGRAM_DURATION.labels(NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN, "removePayload::uuid").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @Override
    public void removeMetadata(final long appId, final String publisherId, final byte[] uuid) throws Exception {
        long startTime = System.nanoTime();
        try {
            METADATA_CACHE.remove(bytesToUUID(uuid));
            String columnFamilyKey = String.format("%d%s%s", appId, dbKeyDelimiter, publisherId);
            ColumnFamilyHandle handle = columnFamilyMap.get(columnFamilyKey);
            if (handle == null) return;
            store.delete(handle, uuid);
        } finally {
            HISTOGRAM_DURATION.labels(String.valueOf(appId), String.valueOf(publisherId), "removeMetadata").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @Override
    public AsyncMetadata deserializeMetadata(final byte[] bytes) throws Exception {
        return toPojo(bytes);
    }

    @Override
    public void processPayload(final Consumer<Pair<byte[], byte[]>> consumer) throws Exception {
        LOGGER.trace("Process Pending Keys - Scheduling Started");
        threadPool.invokeAll(getColumnFamilyHandleStream().map(handle -> (Callable<Void>) () -> {
            RocksIterator iterator = store.newIterator(handle);
            iterator.seekToFirst();
            if (!iterator.isValid()) {
                iterator.close();
                return null;
            }
            String handleKey = UNKNOWN;
            try {
                handleKey = new String(handle.getName(), StandardCharsets.UTF_8);
            } catch (Exception e) {

            }
            if (isTraceEnabled) LOGGER.trace("Started processing rocksdb handle", Map.of("handleKey", handleKey));
            while (iterator.isValid()) {
                try {
                    consumer.accept(Pair.of(iterator.key(), iterator.value()));
                } catch (Exception e) {
                    String iteratorKey;
                    try {
                        iteratorKey = bytesToUUID(iterator.key()).toString();
                    } catch (Exception ignored) {
                        iteratorKey = new String(iterator.key(), StandardCharsets.UTF_8);
                    }
                    LOGGER.error("Error processing rocksdb entry", Map.of("handleKey", handleKey, "iteratorKey", iteratorKey, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                } finally {
                    iterator.next();
                }
            }
            iterator.close();
            return null;
        }).collect(Collectors.toList()));
        LOGGER.trace("Process Pending Keys - Scheduled successfully");
    }

    private Stream<ColumnFamilyHandle> getColumnFamilyHandleStream() {
        return columnFamilyMap.values().stream();
    }

    @Override
    public Map<String, Map<String, Number>> getDbStats() throws Exception {
        Map<String, Map<String, Number>> dbStats = new HashMap<>();
        List<ColumnFamilyHandle> handles = new ArrayList<>(columnFamilyMap.values());
        handles.add(payloadColumnFamilyHandle);
        if (defaultColumnFamilyHandle != null) handles.add(defaultColumnFamilyHandle);
        handles.forEach(handle -> {
            try {
                Map<String, Number> dbCfStats = new HashMap<>();
                dbCfStats.put("size", store.getColumnFamilyMetaData(handle).size());
                dbCfStats.put("fileCount", store.getColumnFamilyMetaData(handle).fileCount());
                dbCfStats.put("estimatedKeyCount", parseLong(store.getProperty(handle, "rocksdb.estimate-num-keys")));
                dbStats.put(new String(store.getColumnFamilyMetaData(handle).name(), StandardCharsets.UTF_8), dbCfStats);
            } catch (RocksDBException e) {
                LOGGER.error("Error getting properties of column family Handle", e);
            }
        });
        return dbStats;
    }

    @Override
    public Map<String, Object> providerInfo() throws Exception {
        return providerInfo;
    }

    @Override
    public List<String> retrieveLoadedPublisherIds() throws Exception {
        return columnFamilyMap.values().stream().map(handle -> {
            try {
                return new String(handle.getName(), StandardCharsets.UTF_8);
            } catch (Exception e) {
                return null;
            }
        }).filter(x -> x != null).collect(Collectors.toList());
    }

    @Override
    public List<String> retrieveLivePayloadKeys(final long appId, final String publisherId) throws Exception {
        String columnFamilyKey = String.format("%d%s%s", appId, dbKeyDelimiter, publisherId);
        ColumnFamilyHandle handle = columnFamilyMap.get(columnFamilyKey);
        if (handle == null) return null;
        RocksIterator iterator = store.newIterator(handle);
        iterator.seekToFirst();
        if (!iterator.isValid()) {
            iterator.close();
            return null;
        }
        List<String> keys = new ArrayList<>();
        while (iterator.isValid()) {
            try {
                keys.add(new String(iterator.key(), StandardCharsets.UTF_8));
            } catch (Exception e) {
            } finally {
                iterator.next();
            }
        }
        iterator.close();
        return keys;
    }

    @Override
    public void gracefulShutDown() {
        try {
            LOGGER.info("Shutting down async persistence provider poller service..");
            pollService.shutdown();
            LOGGER.info("Shutting down async persistence provider thread pool service..");
            threadPool.shutdown();
            LOGGER.info("Checking if rocksdb has any pending payloads; If not will delete the rocksdb file");
            boolean isPending = getColumnFamilyHandleStream().filter(handle -> {
                RocksIterator iterator = store.newIterator(handle);
                iterator.seekToFirst();
                if (!iterator.isValid()) {
                    iterator.close();
                    return false;
                }
                iterator.close();
                return true;
            }).findFirst().isPresent();
            store.close();
            LOGGER.info(String.format("RocksDB pending payloads exist ? - %s", isPending));
            if (!isPending) {
                String path = (String) providerInfo.get("dbPath");
                if (isBlank(path)) return;
                File storeDir = new File(path);
                storeDir.deleteOnExit();
                LOGGER.info("Since no pending payloads exist in rocksdb, scheduling it for delete !!");
            }
        } catch (Exception ignore) {

        }
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private RocksDB getRocksDBInstance(final String fullDbPath, final boolean autoCreate) throws RocksDBException {
        List<ColumnFamilyDescriptor> cfDescriptors = getCfDescriptors(fullDbPath, autoCreate);
        if (cfDescriptors == null || cfDescriptors.isEmpty()) return null;
        //Open the DB in read-write mode and get back all column family handles
        LOGGER.info("Starting RocksDB", Map.of("dbPath", fullDbPath, "dbHost", hostName()));
        List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
        RocksDB db = RocksDB.open(getDbOptions(), fullDbPath, cfDescriptors, columnFamilyHandleList);
        writeOptions = new WriteOptions();
        _ping(db);
        columnFamilyHandleList.forEach(handle -> {
            try {
                String handleName = new String(handle.getName(), StandardCharsets.UTF_8);
                if (handleName.equals(COLUMN_FAMILY_NAME_DEFAULT)) {
                    defaultColumnFamilyHandle = handle;
                    return;
                }
                if (handleName.equals(COLUMN_FAMILY_NAME_PAYLOAD)) {
                    payloadColumnFamilyHandle = handle;
                    return;
                }
                columnFamilyMap.put(columnFamilyName(handle), handle);
            } catch (Exception e) {
                LOGGER.warn("Could not initialize all column handles", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "handleKey", String.valueOf(handle.toString())));
            }
        });
        if (payloadColumnFamilyHandle == null) {
            ColumnFamilyOptions options = getColumnFamilyOptions();
            Map cfOverrides = (Map) optionOverrides.get("payloadColumnFamilyOptions");
            if (cfOverrides != null && !cfOverrides.isEmpty()) options = updateObject(options, cfOverrides);
            if (config().hasPath(payloadCompressionOption)) options.setCompressionType(CompressionType.valueOf(config().getString(payloadCompressionOption)));
            payloadColumnFamilyHandle = db.createColumnFamily(new ColumnFamilyDescriptor(COLUMN_FAMILY_NAME_PAYLOAD.getBytes(StandardCharsets.UTF_8), options));
            columnFamilyHandleList.add(payloadColumnFamilyHandle);
        }
        if (config().hasPath(compactOption) && parseBoolean(config().getString(compactOption))) {
            db.enableFileDeletions(true);
            db.enableAutoCompaction(columnFamilyHandleList);
        }
        return db;
    }

    @SneakyThrows
    private String columnFamilyName(final ColumnFamilyHandle handle) {
        return new String(handle.getName(), StandardCharsets.UTF_8);
    }

    private List<ColumnFamilyDescriptor> getCfDescriptors(final String fullDbPath, final boolean autoCreate) throws RocksDBException {
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

        //Check if an existing DB is present add descriptors for all existing column families
        RocksDB.listColumnFamilies(new Options(), fullDbPath).forEach(columnFamily -> {
            LOGGER.debug("Existing column family name", Map.of("CF", new String(columnFamily, StandardCharsets.UTF_8), "dbHost", hostName()));
            cfDescriptors.add(new ColumnFamilyDescriptor(columnFamily, getColumnFamilyOptions()));
        });

        //If no existing DB found then add the default column family handle
        if (autoCreate && cfDescriptors.size() == 0) cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, getColumnFamilyOptions()));

        return cfDescriptors;
    }

    private void getOrCreateDB(final String baseDbpath, int retryCount) throws RocksDBException, IOException, InterruptedException {
        providerInfo.put("baseDbpath", baseDbpath);
        FileLock lock = this.lock(baseDbpath + "/dhari-rocksdb.lock");
        if (lock == null) {
            LOGGER.warn("Unable to claim rocksdb base path lock.", Map.of("DB", baseDbpath, "Retry Count", retryCount, "dbHost", hostName()));
            Thread.sleep(Duration.ofSeconds(config().getLong(String.format("%s.async.persistence.db.acquire.poll.duration", configPrefix()))).toMillis());
            if (retryCount < config().getInt(String.format("%s.async.persistence.db.acquire.retries", configPrefix()))) {
                LOGGER.warn("Will retry obtaining lock", Map.of("DB", baseDbpath, "Retry Count", retryCount + 1, "dbHost", hostName()));
                getOrCreateDB(baseDbpath, retryCount + 1);
            } else {
                LOGGER.error("Will give up obtaining lock after max retries", Map.of("dbPath", baseDbpath, "Retry Count", retryCount, "dbHost", hostName()));
                throw new IllegalStateException("Unable to claim lock on RocksDB base path");
            }
            return;
        }
        LOGGER.info("Obtained lock on rocksdb", Map.of("dbPath", baseDbpath, "Valid lock", lock.isValid(), "Valid Shared", lock.isShared(), "dbHost", hostName()));
        try {
            Set<Path> dbs;
            String fullDbPath;
            try (Stream<Path> stream = Files.walk(Paths.get(baseDbpath), 1)) {
                dbs = stream.filter(Files::isDirectory).filter(path -> !path.equals(Paths.get(baseDbpath))).collect(Collectors.toSet());
            } catch (IOException e) {
                LOGGER.error("Unable to read directory paths of rocksdb", Map.of("dbPath", baseDbpath, "dbHost", hostName(), "Error", e.getMessage()), e);
                throw e;
            }
            LOGGER.info("Current DBs available", Map.of("DBs", dbs));
            if (dbs != null) {
                for (Path db : dbs) {
                    try {
                        fullDbPath = String.format("%s/%s", baseDbpath, db.getFileName().toString());
                        store = getRocksDBInstance(fullDbPath, false);
                        if (store == null) {
                            LOGGER.warn("Could not load rocksdb !! Path might not be a valid rocksdb dir. Will skip..", Map.of("fullDbPath", fullDbPath));
                            continue;
                        }
                        providerInfo.put("dbPath", fullDbPath);
                        providerInfo.put("dbHost", hostName());
                        LOGGER.info("Claimed rocksdb", providerInfo);
                        break;
                    } catch (RocksDBException e) {
                        LOGGER.warn("Unable to claim rocksdb", Map.of("DB", db, "dbHost", hostName(), "Error", e), e);
                    }
                }
            }
            if (store == null) {
                fullDbPath = String.format("%s/%s", baseDbpath, String.format("rocksdb-%s.db", UUID.randomUUID()));
                try {
                    LOGGER.info("No RocksDBs unclaimed. Starting a new one", Map.of("dbPath", fullDbPath, "dbHost", hostName()));
                    store = getRocksDBInstance(fullDbPath, true);
                    providerInfo.put("dbPath", fullDbPath);
                    providerInfo.put("dbHost", hostName());
                    LOGGER.info("Created & claimed a new rocksdb successfully", providerInfo);
                } catch (RocksDBException e) {
                    LOGGER.error("Failed to create a new RocksDB Instance", Map.of("dbPath", fullDbPath, "dbHost", hostName(), "Error", e.getMessage()), e);
                    throw e;
                }
            }
        } finally {
            lock.release();
        }
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    private DBOptions getDbOptions() {
        DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setUseFsync(true);
        Map dbOverrides = (Map) optionOverrides.get("dbOptions");
        if (dbOverrides == null || dbOverrides.isEmpty()) return options;
        return updateObject(options, dbOverrides);
    }

    private FileLock lock(final String lockFilePath) throws IOException {
        final RandomAccessFile file = new RandomAccessFile(new File(lockFilePath), "rw");
        final FileLock fileLock = file.getChannel().tryLock();
        //if the JVM is not able to acquire a lock then a null
        //is returned, it could be because the lock is already acquired
        //by another thread or process.
        return fileLock;
    }

}
