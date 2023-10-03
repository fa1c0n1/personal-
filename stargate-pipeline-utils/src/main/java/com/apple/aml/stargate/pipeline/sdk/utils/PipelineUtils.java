package com.apple.aml.stargate.pipeline.sdk.utils;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DerivedSchemaOptions;
import com.apple.aml.stargate.common.options.FieldExtractorMappingOptions;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.options.RetryOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.utils.PrometheusUtils;
import com.apple.aml.stargate.common.utils.SchemaUtils;
import com.apple.aml.stargate.common.utils.SneakyRetryFunction;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.pipeline.inject.DhariIngestionService;
import com.apple.aml.stargate.pipeline.inject.GuiceModule;
import com.apple.aml.stargate.pipeline.sdk.io.hadoop.HdfsSharedConfiguration;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.inject.Guice;
import com.google.inject.Injector;
import freemarker.ext.beans.BeansWrapper;
import io.github.resilience4j.retry.Retry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.event.CacheEntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.STATICS;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_RM_HOST;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_RM_INITIALIZATION_VECTOR;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_RM_TOKEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.METRIC_ATTRIBUTE_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ATTRIBUTE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ATTRIBUTE_VALUE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.EXPR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.STAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_COUNTER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_EXPR_COUNTER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_EXPR_GAUGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_EXPR_HISTOGRAM;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_GAUGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_HISTOGRAM;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_HISTOGRAM_BYTES;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_HISTOGRAM_DURATION;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.PIPELINE_HISTOGRAM_SIZE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.NOT_APPLICABLE_HYPHEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_BASE_SHARED_DIRECTORY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_ENABLE_DYNAMIC_KERBEROS_LOGIN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_ENABLE_INSECURE_HMS;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_HDFS_SHARED_CONFIG_FILE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_LOCAL_RM_URI;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_REMOTE_RM_URI;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_SHARED_DIRECTORY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.sharedToken;
import static com.apple.aml.stargate.common.constants.PipelineConstants.Krb5Properties.AUTH_LOGIN_CONFIG;
import static com.apple.aml.stargate.common.constants.PipelineConstants.Krb5Properties.KRB5_DEBUG;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.SPARK_HADOOP_PROP_PREFIX;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.ClassUtils.fetchClassIfExists;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.CsvUtils.csvMapper;
import static com.apple.aml.stargate.common.utils.EncryptionUtils.aesNullableKeySpec;
import static com.apple.aml.stargate.common.utils.EncryptionUtils.defaultEncrypt;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readNullableJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.NetworkUtils.hostName;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.counterMetric;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.gaugeMetric;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.histogramMetric;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.initializeMetrics;
import static com.apple.aml.stargate.pipeline.sdk.io.hadoop.HdfsSharedConfiguration.SYS_PROP_KRB5_CONF;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.fetchDerivedSchema;
import static com.apple.amp.schemastore.constants.SchemaStoreConstants.LATEST_VERSION;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Integer.parseInt;
import static java.util.Collections.singletonList;

public final class PipelineUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final Logger RETRY_LOGGER = LoggerFactory.getLogger("stargate.RetryHandler");
    private static final AtomicReference<Configuration> HADOOP_RUNTIME_CONF = new AtomicReference<>();
    private static final AtomicReference<Triple<Configuration, Map<String, String>, Set<String>>> HADOOP_CONF_REF = new AtomicReference<>();
    private static final AtomicReference<HdfsSharedConfiguration> HDFS_SHARED_CONF = new AtomicReference<>();
    private static final boolean sparkConfExists = fetchClassIfExists("org.apache.spark.SparkConf") != null;
    private static final Class sparkUtilsClass = sparkConfExists ? fetchClassIfExists("com.apple.aml.stargate.runners.spark.utils.SparkUtils") : null;
    private static final boolean dynamic_kerberos_hms_login_enabled = parseBoolean(env(STARGATE_ENABLE_DYNAMIC_KERBEROS_LOGIN, "false")) || parseBoolean(env(STARGATE_ENABLE_INSECURE_HMS, "false"));
    private static final Cache<String, String> KERBEROS_LOGIN_CACHE = new Cache2kBuilder<String, String>() {
    }.entryCapacity(10).expireAfterWrite((config().hasPath("stargate.cache.kerberos.login.expiry") ? config().getInt("stargate.cache.kerberos.login.expiry") : 30), TimeUnit.MINUTES).addAsyncListener((CacheEntryExpiredListener<String, String>) (cache, entry) -> {
        LOGGER.trace("Kerberos Login cache entry about to expire. Will process this event now if applicable", Map.of("principal", entry.getKey(), "keytab", entry.getValue()));
        try {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            LOGGER.trace("Current HDFS Kerberos User Details", Map.of("user", ugi.getUserName(), "kerberosEnabled", ugi.hasKerberosCredentials()));
            String principal = ugi.getUserName().split("@")[0];
            if (ugi.hasKerberosCredentials() && entry.getKey().equals(principal)) {
                LOGGER.debug("Renewing HDFS Kerberos started for ", Map.of("principal", principal));
                ugi.checkTGTAndReloginFromKeytab();
                LOGGER.debug("Renewing HDFS Kerberos completed", Map.of("principal", principal));
                cache.put(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            LOGGER.warn("Error in renewing HDFS Kerberos!!", e);
        }
    }).build();
    private static final Cache<String, Schema> LOCAL_SCHEMA_CACHE = new Cache2kBuilder<String, Schema>() {
    }.entryCapacity(100).expireAfterWrite((config().hasPath("stargate.cache.local.schema.expiry") ? config().getInt("stargate.cache.local.schema.expiry") : 60), TimeUnit.MINUTES).build();

    private static final AtomicReference<Boolean> SOLR_SHARED_CONF = new AtomicReference<>();
    private static final SecretKeySpec STARGATE_RM_KEY_SPEC = aesNullableKeySpec(sharedToken());
    private static String workerId = defaultWorkerId();
    private static Counter counter;
    private static Histogram histogram;
    private static Histogram histogramDuration;
    private static Histogram histogramBytes;
    private static Histogram histogramSize;
    private static Gauge gauge;
    private static Counter exprCounter;
    private static Histogram exprHistogram;
    private static Gauge exprGauge;

    static {
        try {
            initializeMetrics();
            counter = counterMetric(PIPELINE_COUNTER, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            histogram = histogramMetric(PIPELINE_HISTOGRAM, null, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            histogramDuration = histogramMetric(PIPELINE_HISTOGRAM_DURATION, null, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            histogramBytes = histogramMetric(PIPELINE_HISTOGRAM_BYTES, String.valueOf(1024), NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            histogramSize = histogramMetric(PIPELINE_HISTOGRAM_SIZE, null, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            gauge = gaugeMetric(PIPELINE_GAUGE, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            exprCounter = counterMetric(PIPELINE_EXPR_COUNTER, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            exprHistogram = histogramMetric(PIPELINE_EXPR_HISTOGRAM, null, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
            exprGauge = gaugeMetric(PIPELINE_EXPR_GAUGE, NODE_NAME, NODE_TYPE, SCHEMA_ID, STAGE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PipelineUtils() {
    }

    private static String defaultWorkerId() {
        String hostName = hostName();
        for (final String keyword : new String[]{env(STARGATE_PIPELINE_ID, "---"), "spark", "flink", "taskmanager", "executor", "worker"}) {
            hostName = hostName.replaceAll(keyword.toLowerCase(), EMPTY_STRING);
        }
        return hostName.replaceAll("(--+)", EMPTY_STRING);
    }

    public static String rootNodeName(final String name) {
        int index = name.indexOf(":");
        if (index <= 0) return name;
        return name.substring(0, index);
    }

    public static String workerId() {
        return workerId;
    }

    public static String workerId(final String id) {
        workerId = id;
        return workerId;
    }

    public static Class sparkUtilsClass() {
        return sparkUtilsClass;
    }

    @SuppressWarnings("unchecked")
    public static void appendHadoopConfiguration(final Map<String, String> properties, final List<String> resources) throws Exception {
        Triple<Configuration, Map<String, String>, Set<String>> config = getHadoopConfigReference();
        Configuration conf = config.getLeft();
        Map<String, String> map = config.getMiddle();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            if (!key.startsWith(SPARK_HADOOP_PROP_PREFIX)) {
                continue;
            }
            key = key.substring(SPARK_HADOOP_PROP_PREFIX.length());
            conf.set(key, value);
            map.put(key, value);
        }
        if (resources != null) {
            for (final String resource : resources) {
                conf.addResource(new Path("file://" + resource));
            }
            config.getRight().addAll(resources);
        }
        if (sparkUtilsClass != null) {
            sparkUtilsClass.getDeclaredMethod("appendSparkHadoopProperties", Map.class, List.class).invoke(null, properties, resources);
        }
    }

    @SuppressWarnings("unchecked")
    public static Triple<Configuration, Map<String, String>, Set<String>> getHadoopConfigReference() throws Exception {
        Triple<Configuration, Map<String, String>, Set<String>> config = HADOOP_CONF_REF.get();
        if (config != null) {
            return config;
        }
        if (sparkUtilsClass == null) {
            HADOOP_CONF_REF.compareAndSet(null, Triple.of(new Configuration(), new HashMap<>(), new HashSet<>()));
        } else {
            Configuration conf = null;
            try {
                conf = (Configuration) sparkUtilsClass.getDeclaredMethod("getSparkHadoopConfig").invoke(null);
            } catch (Exception e) {
                LOGGER.trace("Could not fetch hdfs config from getSparkHadoopConfig. Reason : " + e.getMessage());
            }
            HADOOP_CONF_REF.compareAndSet(null, Triple.of(conf == null ? new Configuration() : conf, new HashMap<>(), new HashSet<>()));
        }
        return HADOOP_CONF_REF.get();
    }

    public static Configuration hiveConfiguration() {
        return hadoopConfiguration();
    }

    @SneakyThrows
    public static Configuration hadoopConfiguration() {
        Configuration configuration = HADOOP_RUNTIME_CONF.get();
        if (configuration == null) {
            return getHadoopConfigReference().getLeft();
        }
        return configuration;
    }

    public static boolean isDynamicKerberosLoginEnabled() {
        return dynamic_kerberos_hms_login_enabled;
    }

    public static void performKerberosLogin() throws Exception {
        performKerberosLogin(true);
    }

    public static void performKerberosLogin(final boolean setAsRuntime) throws Exception { // This assumes kerberos is used only by Hadoop/Hive module and piggybacks on `UserGroupInformation`. Need to take care of non hadoop krb use-cases too
        Triple<Configuration, Map<String, String>, Set<String>> hdfsConfig = PipelineUtils.getHadoopConfigReference();
        Configuration conf = hdfsConfig.getLeft();
        HdfsSharedConfiguration config = HDFS_SHARED_CONF.get();
        if (config == null) {
            synchronized (PipelineUtils.class) {
                LOGGER.debug("HDFSConfig not loaded in memory.. loading it now");
                String sharedDir = getSharedDir();
                java.nio.file.Path hdfsConfigFile = Paths.get(sharedDir, STARGATE_HDFS_SHARED_CONFIG_FILE);
                if (!hdfsConfigFile.toFile().exists()) {
                    LOGGER.debug("HDFSConfig file doesn't exist!!", Map.of("fileName", hdfsConfigFile.toString()));
                    String remoteRmUri = env(STARGATE_REMOTE_RM_URI, null);
                    if (remoteRmUri != null && !remoteRmUri.isBlank()) {
                        downloadHdfsResourcesFromRemoteRmServer(remoteRmUri.trim(), sharedDir);
                    }
                    LOGGER.debug("HDFSConfig file doesn't exist!!. Cannot perform Kerberos Login", Map.of("fileName", hdfsConfigFile.toString()));
                    HDFS_SHARED_CONF.compareAndSet(null, new HdfsSharedConfiguration());
                    return;
                }
                String jsonString = Files.readString(hdfsConfigFile);
                if (isBlank(jsonString)) {
                    LOGGER.debug("HDFSConfig file is empty!!. Cannot perform Kerberos Login", Map.of("fileName", hdfsConfigFile.toString()));
                    HDFS_SHARED_CONF.compareAndSet(null, new HdfsSharedConfiguration());
                    return;
                }
                config = readJson(jsonString, HdfsSharedConfiguration.class);
                if (!isBlank(config.getKrb5FileName())) {
                    System.setProperty(SYS_PROP_KRB5_CONF, config.getKrb5FileName());
                }
                if (config.getConfig() != null) {
                    config.getConfig().entrySet().forEach(e -> conf.set(e.getKey(), e.getValue()));
                }
                if (!isBlank(config.getCoreXmlFileName())) {
                    conf.addResource(new Path("file://" + config.getCoreXmlFileName()));
                }
                if (!isBlank(config.getHdfsXmlFileName())) {
                    conf.addResource(new Path("file://" + config.getHdfsXmlFileName()));
                }
                if (!isBlank(config.getHiveXmlFileName())) {
                    conf.addResource(new Path("file://" + config.getHiveXmlFileName()));
                }
                if (!isBlank(config.getPrincipal())) {
                    System.setProperty("security.kerberos.login.principal", config.getPrincipal().trim());
                }
                if (!isBlank(config.getKeytabFileName())) {
                    System.setProperty("security.kerberos.login.keytab", config.getKeytabFileName());
                }
                HDFS_SHARED_CONF.compareAndSet(null, config);
                UserGroupInformation.setConfiguration(conf);
                if (setAsRuntime && HADOOP_RUNTIME_CONF.get() == null) {
                    PipelineUtils.setHadoopRuntimeConf(conf);
                }
            }
        }
        String principal = config.getPrincipal();
        if (isBlank(principal)) {
            return;
        }
        String keytab = config.getKeytabFileName();
        if (isBlank(keytab)) {
            return;
        }
        String krb5 = config.getKrb5FileName();
        if (keytab.equals(KERBEROS_LOGIN_CACHE.get(principal))) {
            return; // logged in recently based on cache policy
        }
        LOGGER.debug("Performing Kerberos Login", Map.of("principal", principal, "keytab", keytab, "krb5", krb5));
        System.setProperty(SYS_PROP_KRB5_CONF, config.getKrb5FileName());
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        currentUser.addCredentials(UserGroupInformation.getLoginUser().getCredentials());
        if (setAsRuntime && HADOOP_RUNTIME_CONF.get() == null) {
            PipelineUtils.setHadoopRuntimeConf(conf);
        }
        KERBEROS_LOGIN_CACHE.put(principal, keytab);
        LOGGER.debug("Kerberos Login Successful", Map.of("principal", principal, "keytab", keytab, "krb5", krb5));
    }

    public static String getSharedDir() {
        String sharedDir = env(STARGATE_SHARED_DIRECTORY, null);
        if (isBlank(sharedDir)) {
            String baseSharedDirectory = env(STARGATE_BASE_SHARED_DIRECTORY, null);
            sharedDir = baseSharedDirectory + "/" + pipelineId();
        }
        return sharedDir;
    }

    private static void downloadHdfsResourcesFromRemoteRmServer(final String uri, final String sharedDir) {
        LOGGER.info("Downloading HDFS resource file(s) from remote rm host", Map.of("uri", uri));
        try {
            Files.createDirectories(Paths.get(sharedDir));
            String configUrl = String.format("%s/resource/%s", uri, STARGATE_HDFS_SHARED_CONFIG_FILE);
            LOGGER.debug("Downloading HDFS resource from remote rm host", Map.of("uri", configUrl, "fileName", STARGATE_HDFS_SHARED_CONFIG_FILE));
            ByteBuffer buffer = WebUtils.getData(configUrl, resourceManagerAuthHeaders(), ByteBuffer.class, false);
            if (buffer == null || buffer.array() == null || buffer.array().length == 0) {
                throw new GenericException("File not found/empty file response from remote rm host", Map.of("configUrl", configUrl));
            }
            java.nio.file.Path hdfsConfigFilePath = Paths.get(sharedDir, STARGATE_HDFS_SHARED_CONFIG_FILE);
            if (!hdfsConfigFilePath.toFile().exists()) {
                Files.write(hdfsConfigFilePath, buffer.array(), StandardOpenOption.CREATE);
            }
            String jsonString = Files.readString(hdfsConfigFilePath);
            Map<Object, Object> map = readNullableJsonMap(jsonString);
            if (map == null || map.isEmpty()) {
                LOGGER.debug("No HDFS resource file(s) configured inside configPath !", Map.of("uri", uri, "configUrl", configUrl, "content", String.valueOf(jsonString)));
                return;
            }
            map.entrySet().stream().filter(e -> e.getValue() != null && e.getKey().toString().endsWith("FileName") && !e.getValue().toString().trim().isBlank()).parallel().forEach(e -> {
                String fileName = e.getValue().toString().trim().replaceAll(sharedDir, EMPTY_STRING).trim();
                if (fileName.isBlank() || fileName.length() == 1) {
                    return;
                }
                fileName = fileName.substring(1);
                downloadResourcesFromRemoteRmServer(uri, sharedDir, fileName);
            });
        } catch (Exception e) {
            LOGGER.warn("Could not download all HDFS resource file(s) from remote rm host", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "uri", uri), e);
        }
    }

    public static void setHadoopRuntimeConf(Configuration conf) {
        if (conf != null) {
            HADOOP_RUNTIME_CONF.set(conf);
        }
    }

    public static Map<String, String> resourceManagerAuthHeaders() throws Exception {
        String hostName = hostName();
        String rmToken = String.join("~", hostName, pipelineId(), String.valueOf(System.currentTimeMillis()));
        Pair<String, String> encryptPair = defaultEncrypt(rmToken, resourceManagerKeySpec());
        String encryptedToken = encryptPair.getLeft();
        String initVec = encryptPair.getRight();

        return Map.of(HEADER_RM_HOST, hostName, HEADER_RM_TOKEN, encryptedToken, HEADER_RM_INITIALIZATION_VECTOR, initVec);
    }

    private static void downloadResourcesFromRemoteRmServer(final String uri, final String sharedDir, final String fileName) {
        String fileUrl = String.format("%s/resource/%s", uri, fileName);
        LOGGER.debug("Downloading resource from remote rm host", Map.of("uri", fileUrl, "fileName", fileName));
        try {
            ByteBuffer fileBuffer = WebUtils.getData(fileUrl, resourceManagerAuthHeaders(), ByteBuffer.class, false);
            if (fileBuffer == null || fileBuffer.array() == null || fileBuffer.array().length == 0) {
                throw new GenericException("File not found/empty file response from remote rm host", Map.of("fileUrl", fileUrl));
            }
            java.nio.file.Path path = Paths.get(sharedDir, fileName);
            if (!path.toFile().exists()) {
                Files.write(path, fileBuffer.array(), StandardOpenOption.CREATE);
            }
        } catch (Exception ex) {
            LOGGER.warn("Could not download resource from remote host", Map.of(ERROR_MESSAGE, String.valueOf(ex.getMessage()), "fileUrl", fileUrl, "fileName", fileName), ex);
            throw new RuntimeException(ex);
        }
    }

    public static SecretKeySpec resourceManagerKeySpec() {
        return STARGATE_RM_KEY_SPEC;
    }

    public static void performSolrKrb5Login(final Map<String, String> files, final boolean debug) throws Exception {
        Boolean config = SOLR_SHARED_CONF.get();
        if (config == null || !config) {
            synchronized (PipelineUtils.class) {
                String uri = env(STARGATE_REMOTE_RM_URI, null);
                if (isBlank(uri)) {
                    return;
                }
                String sharedDir = getSharedDir();
                Files.createDirectories(Paths.get(sharedDir));
                for (String fileName : files.values()) {
                    if (fileName.isBlank() || fileName.length() == 1) {
                        return;
                    }
                    java.nio.file.Path configFile = Paths.get(sharedDir, fileName);
                    if (!configFile.toFile().exists()) {
                        LOGGER.info("Downloading SOLR resource file from remote rm host", Map.of("uri", uri, "fileName", fileName));
                        downloadResourcesFromRemoteRmServer(uri, sharedDir, fileName);
                    }
                }
                LOGGER.debug("Performing SOLR Login");
                setSolrKrb5SystemProps(files.get("JAAS"), files.get("KRB5"), debug);
                SOLR_SHARED_CONF.compareAndSet(null, Boolean.TRUE);
            }
        }
    }

    public static void setSolrKrb5SystemProps(final String jaasFileName, final String krb5FileName, final boolean debug) {
        System.setProperty(KRB5_DEBUG, String.valueOf(debug)); // set to true if you want to debug krb auth issues
        System.setProperty(AUTH_LOGIN_CONFIG, getSharedDir() + "/" + jaasFileName);
        System.setProperty(SYS_PROP_KRB5_CONF, getSharedDir() + "/" + krb5FileName);
    }

    @SuppressWarnings("deprecation")
    public static byte[] getKrb5FileContent(final StargateNode node, final String krb5, final String krb5FileName, final String sharedDir, final String keytabFileName) throws Exception {
        byte[] krb5FileContent;
        if (!isBlank(krb5)) {
            String content = krb5.strip();
            if (content.contains("${")) {
                String krb5TemplateName = node.getName() + "~KRB5";
                loadFreemarkerTemplate(krb5TemplateName, content);
                freemarker.template.Configuration cfg = freeMarkerConfiguration();
                Map<String, Object> object = Map.of("krb5", krb5, krb5FileName, krb5FileName, "runtime", Map.of("sharedDirectoryPath", sharedDir, "keytabFilePath", keytabFileName), STATICS, BeansWrapper.getDefaultInstance().getStaticModels());
                StringWriter writer = new StringWriter();
                cfg.getTemplate(krb5TemplateName).process(object, writer);
                content = writer.toString().strip() + "\n";
            } else {
                content = getModifiedKrb5Content(sharedDir, keytabFileName, content);
            }
            krb5FileContent = content.getBytes(StandardCharsets.UTF_8);
        } else {
            krb5FileContent = getModifiedKrb5Content(sharedDir, keytabFileName, new String((byte[]) node.configFiles().get(krb5FileName), StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        }
        return krb5FileContent;
    }

    private static String getModifiedKrb5Content(final String sharedDir, final String keytabFileName, final String input) {
        String content = input.strip();
        content = content + "\n[replacement_token]";
        content = content.replaceAll("\\n(([\\t ]*)default_keytab_name([\\t ]*)=([\\t ]*)([^\\n]+))", EMPTY_STRING);
        content = content.replaceAll("\\[logging\\]([^\\[]*)", EMPTY_STRING);
        content = content.replaceAll("\\[replacement_token\\]", EMPTY_STRING);
        content += "\n[logging]";
        content += "\n\tdefault = FILE:" + sharedDir + "/krb5libs.log";
        content += "\n\tkdc = FILE:" + sharedDir + "/krb5kdc.log";
        content += "\n\tadmin_server = FILE:" + sharedDir + "/kadmind.log";
        content = content.replaceAll("(\\[libdefaults\\])", "$1\n\tdefault_keytab_name = " + keytabFileName);
        content = content.strip() + "\n";
        return content;
    }

    public static Map saveState(final String stateId, final Map state) throws Exception {
        return saveState(stateId, state, Map.class);
    }

    public static <O> O saveState(final String stateId, final O state, final Class<O> returnType) throws Exception {
        String resourceManagerUri = env(STARGATE_REMOTE_RM_URI, env(STARGATE_LOCAL_RM_URI, null));
        String url = String.format("%s/state/save/%s", resourceManagerUri, stateId);
        return WebUtils.httpPost(url, state, null, resourceManagerAuthHeaders(), returnType, false);
    }

    public static Map getState(final String stateId) throws Exception {
        return getState(stateId, Map.class);
    }

    public static <O> O getState(final String stateId, final Class<O> returnType) throws Exception {
        String resourceManagerUri = env(STARGATE_REMOTE_RM_URI, env(STARGATE_LOCAL_RM_URI, null));
        String url = String.format("%s/state/get/%s", resourceManagerUri, stateId);
        return WebUtils.httpPost(url, null, null, resourceManagerAuthHeaders(), returnType, false);
    }

    @SneakyThrows
    public static Map saveLocalSchema(final String schemaId, final Object schema) {
        String resourceManagerUri = env(STARGATE_REMOTE_RM_URI, env(STARGATE_LOCAL_RM_URI, null));
        String url = String.format("%s/schema/save/%s", resourceManagerUri, schemaId);
        Map response = WebUtils.httpPost(null, url, schema, null, resourceManagerAuthHeaders(), Map.class, false, false);
        Object avro = response;
        if (response == null || response.isEmpty()) {
            LOGGER.warn("Could not save schema to resource manager!! Will try saving it again with log enabled !!");
            response = WebUtils.httpPost(null, url, schema, null, resourceManagerAuthHeaders(), Map.class, false, true);
            if (response == null || response.isEmpty()) {
                LOGGER.warn("Could not save schema to resource manager!! Dynamic schema's might not function properly!!");
                avro = schema;
            }
        }
        Schema avroSchema = new Schema.Parser().parse(avro instanceof String ? (String) avro : jsonString(avro));
        LOCAL_SCHEMA_CACHE.computeIfAbsent(schemaId, s -> avroSchema);
        return response;
    }

    @SneakyThrows
    public static Schema getLocalSchema(final String schemaId) {
        return LOCAL_SCHEMA_CACHE.computeIfAbsent(schemaId, s -> fetchLocalSchema(schemaId));
    }

    @SneakyThrows
    private static Schema fetchLocalSchema(final String schemaId) {
        String resourceManagerUri = env(STARGATE_REMOTE_RM_URI, env(STARGATE_LOCAL_RM_URI, null));
        String url = String.format("%s/schema/get/%s", resourceManagerUri, schemaId);
        Map schemaMap = WebUtils.httpPost(url, null, null, resourceManagerAuthHeaders(), Map.class, false);
        return new Schema.Parser().parse(jsonString(schemaMap));
    }

    @SneakyThrows
    public static Schema fetchSchemaWithLocalFallback(final String schemaReference, final String input) {
        return SchemaUtils.fetchSchema(schemaReference, input, LATEST_VERSION, () -> getLocalSchema(input));
    }

    public static Schema getInternalSchema(PipelineConstants.ENVIRONMENT environment, final String fileName, final String defaultSchemaString) {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = getInternalSchemaString(fileName, defaultSchemaString).replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    private static String getInternalSchemaString(final String fileName, final String defaultSchemaString) {
        try {
            String schemaString = IOUtils.resourceToString("/" + fileName, Charset.defaultCharset());
            if (schemaString == null || schemaString.isBlank()) {
                throw new Exception("Could not load schemaString");
            }
            return schemaString;
        } catch (Exception e) {
            return defaultSchemaString;
        }
    }

    public static Retry retrySettings(final String nodeName, final RetryOptions retryOptions) {
        return SneakyRetryFunction.retry(retryOptions, e -> {
            RETRY_LOGGER.warn("Error invoking supplied logic. Will retry accordingly", Map.of(NODE_NAME, nodeName, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            return true;
        });
    }

    @SuppressWarnings("unchecked")
    public static List<FieldExtractorMappingOptions> fetchExtractorFields(final Object input, final String schemaId) throws IOException {
        Map<String, FieldExtractorMappingOptions> fieldMap = new LinkedHashMap<>();
        Schema schema = null;
        if (!isBlank(schemaId)) {
            schema = fetchSchemaWithLocalFallback(null, schemaId);
            fieldMap.putAll(schema.getFields().stream().map(f -> {
                FieldExtractorMappingOptions field = new FieldExtractorMappingOptions();
                field.setFieldName(f.name());
                return field;
            }).collect(Collectors.toMap(FieldExtractorMappingOptions::getFieldName, x -> x)));
        }
        if (input != null) {
            ObjectReader csvReader = csvMapper().readerFor(FieldExtractorMappingOptions.class).with(csvMapper().schemaFor(FieldExtractorMappingOptions.class));
            if (input instanceof String) {
                MappingIterator<FieldExtractorMappingOptions> iterator = csvReader.readValues(((String) input).trim());
                fieldMap.putAll(iterator.readAll().stream().collect(Collectors.toMap(FieldExtractorMappingOptions::getFieldName, x -> x)));
            } else if (input instanceof Map) {
                for (Map.Entry<String, Object> entry : ((Map<String, Object>) input).entrySet()) {
                    String fieldName = entry.getKey().trim();
                    if (fieldName.contains("\\.")) throw new UnsupportedOperationException(String.format("key (%s) cannot be hierarchical; only mappings can be hierarchical", fieldName));
                    FieldExtractorMappingOptions field;
                    if (entry.getValue() == null || isBlank(String.valueOf(entry.getValue()))) {
                        field = new FieldExtractorMappingOptions();
                    } else if (entry.getValue() instanceof String) {
                        field = csvReader.readValue(String.format("%s,%s", fieldName, entry.getValue()));
                    } else {
                        field = readJson(jsonString(entry.getValue()), FieldExtractorMappingOptions.class);
                    }
                    field.setFieldName(fieldName);
                    fieldMap.put(fieldName, field);
                }
            } else {
                throw new UnsupportedOperationException("fields should be either a csv or a map");
            }
        }
        List<FieldExtractorMappingOptions> fields = new ArrayList<>();
        if (schema == null) {
            fields.addAll(fieldMap.values());
        } else {
            schema.getFields().forEach(f -> fields.add(fieldMap.get(f.name())));
        }
        fields.stream().filter(f -> isBlank(f.getMapping())).forEach(f -> f.setMapping(f.getFieldName()));
        return fields;
    }

    @SneakyThrows
    public static Schema fetchTargetSchema(final List<FieldExtractorMappingOptions> fields, final Schema schema) {
        LinkedHashMap<String, Schema.Field> fieldsMap = new LinkedHashMap<>();
        for (FieldExtractorMappingOptions mapping : fields) {
            Schema.Field field = null;
            for (String token : mapping.getMapping().split("\\.")) {
                try {
                    if (field == null) field = schema.getField(token);
                    else field = field.schema().getField(token);
                    if (field == null) break;
                } catch (Exception | Error e) {
                    field = null;
                    break;
                }
            }
            if (field == null) {
                Map<String, Object> spec = new LinkedHashMap<>();
                spec.put("type", "record");
                spec.put("name", mapping.getFieldName());
                spec.put("fields", singletonList(mapping.avroSpec()));
                String schemaString = jsonString(spec);
                Schema.Parser parser = new Schema.Parser();
                Schema fSchema = parser.parse(schemaString);
                field = fSchema.getFields().get(0);
            }
            fieldsMap.put(mapping.getFieldName(), field);
        }
        List<Schema.Field> targetFields = new ArrayList<>();
        fieldsMap.forEach((k, f) -> targetFields.add(new Schema.Field(k, f.schema(), f.doc(), f.defaultVal())));
        return Schema.createRecord(schema.getName(), schema.getDoc(), String.format("%s.extracted", schema.getNamespace()).replace('-', '_'), schema.isError(), targetFields);
    }

    public static KV<String, GenericRecord> consumeOutput(final KV<String, GenericRecord> kv, final String responseKey, final Object response, final Schema schema, final String recordSchemaKey, final String targetSchemaKey, final ObjectToGenericRecordConverter converter, final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap, final String nodeName, final String nodeType, final Consumer<KV<String, GenericRecord>> consumer) throws Exception {
        String key = null;
        Object value;
        GenericRecord output = null;
        try {
            if (response instanceof Pair) {
                Pair pair = (Pair) response;
                key = pair.getKey().toString();
                value = pair.getValue();
            } else if (response instanceof Map.Entry) {
                Map.Entry entry = (Map.Entry) response;
                key = entry.getKey().toString();
                value = entry.getValue();
            } else if (response instanceof GenericRecord) {
                if (((GenericRecord) response).getSchema().getFullName().equals(targetSchemaKey == null ? EMPTY_STRING : targetSchemaKey.split(SCHEMA_DELIMITER)[0])) {
                    key = responseKey == null ? kv.getKey() : responseKey;
                    value = response;
                    output = (GenericRecord) response;
                } else {
                    key = responseKey == null ? kv.getKey() : responseKey;
                    value = response.toString();
                }
            } else {
                key = responseKey == null ? kv.getKey() : responseKey;
                value = response;
            }
            if (key == null) return null;
            String[] targetSchemaTokens = (targetSchemaKey == null ? recordSchemaKey : targetSchemaKey).split(SCHEMA_DELIMITER);
            if (output == null) {
                output = (targetSchemaKey == null ? converterMap.computeIfAbsent(recordSchemaKey, s -> converter(schema)) : converter).convert(value);
                if (output == null) return null;
                if (targetSchemaTokens.length >= 2) output = new AvroRecord(output, parseInt(targetSchemaTokens[1]));
            }
            incCounters(nodeName, nodeType, targetSchemaTokens[0], ELEMENTS_OUT, SOURCE_SCHEMA_ID, recordSchemaKey.split(SCHEMA_DELIMITER)[0]);
            KV<String, GenericRecord> returnKV = KV.of(key, output);
            consumer.accept(returnKV);
            return returnKV;
        } catch (Exception e) {
            LOGGER.warn("Error in converting output to next node", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", String.valueOf(key), NODE_NAME, nodeName, NODE_TYPE, nodeType, "recordSchemaKey", String.valueOf(recordSchemaKey), "targetSchemaKey", String.valueOf(targetSchemaKey)));
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    public static void consumeOutputRecords(final String nodeName, final String nodeType, @DoFn.Element final KV<String, GenericRecord> kv, final GenericRecord record, final Schema recordSchema, final String recordSchemaId, final Object response, final boolean emit, final DerivedSchemaOptions options, final ConcurrentHashMap<String, Schema> schemaMap, final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap, final String schemaId, final ObjectToGenericRecordConverter converter, final Consumer<KV<String, GenericRecord>> consumer) throws Exception {
        int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        String recordSchemaKey = String.format("%s:%d", recordSchemaId, version);
        Schema targetSchema;
        String targetSchemaKey;
        if (emit && options.deriveSchema()) {
            targetSchema = fetchDerivedSchema(options, nodeName, recordSchemaKey, version, schemaMap, converterMap);
            targetSchemaKey = String.format("%s:%d", targetSchema.getFullName(), version);
        } else {
            targetSchema = recordSchema;
            targetSchemaKey = schemaId == null ? recordSchemaId : schemaId;
        }
        ObjectToGenericRecordConverter resolvedConverter = converter != null ? converter : converterMap.computeIfAbsent(targetSchemaKey, s -> converter(fetchSchemaWithLocalFallback(options.getSchemaReference(), targetSchemaKey)));
        if (response instanceof Collection) {
            for (Object obj : (Collection<Object>) response) {
                consumeOutput(kv, null, obj, targetSchema, recordSchemaKey, targetSchemaKey, resolvedConverter, converterMap, nodeName, nodeType, consumer);
            }
        } else {
            consumeOutput(kv, null, response, targetSchema, recordSchemaKey, targetSchemaKey, resolvedConverter, converterMap, nodeName, nodeType, consumer);
        }
    }

    public static Injector getInjector(final String nodeName, final JavaFunctionOptions options, final NodeService nodeService, final ErrorService errorService) throws Exception {
        GuiceModule module = new GuiceModule(nodeName, Class.forName(options.getClassName()), options, PrometheusUtils.metricsService(), nodeService, errorService, new DhariIngestionService(options.getDhariOptions()), options.context());
        Injector injector = Guice.createInjector(module);
        module.setInjector(injector);
        return injector;
    }

    public static void incCounters(final String nodeName, final String nodeType, final String schemaId, final String stage, final String... attributeKVs) {
        incCounters(nodeName, nodeType, schemaId, stage, 1, attributeKVs);
    }

    public static void incCounters(final String nodeName, final String nodeType, final String schemaId, final String stage, final double amt, final String... attributeKVs) {
        counter(nodeName, nodeType, schemaId, stage).inc(amt);
        for (int i = 0; i < attributeKVs.length; ) {
            String attributeName = attributeKVs[i++];
            String attributeValue = attributeKVs[i++];
            counter(nodeName, nodeType, schemaId, stage, attributeName, attributeValue).inc(amt);
        }
    }

    public static Counter.Child counter(final String nodeName, final String nodeType, final String schemaId, final String stage) {
        return counter(nodeName, nodeType, schemaId, stage, NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN);
    }

    public static Counter.Child counter(final String nodeName, final String nodeType, final String schemaId, final String stage, final String attributeName, final String attributeValue) {
        return counter.labels(nodeName, nodeType, schemaId, stage, attributeName, attributeValue);
    }

    public static Histogram.Child histogram(final String nodeName, final String nodeType, final String schemaId, final String stage) {
        return histogram.labels(nodeName, nodeType, schemaId, stage, NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN);
    }

    public static Histogram.Child histogram(final String nodeName, final String nodeType, final String schemaId, final String stage, final String attributeName, final String attributeValue) {
        return histogram.labels(nodeName, nodeType, schemaId, stage, attributeName, attributeValue);
    }

    public static Histogram.Child histogramDuration(final String nodeName, final String nodeType, final String schemaId, final String stage) {
        return histogramDuration.labels(nodeName, nodeType, schemaId, stage, NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN);
    }

    public static Histogram.Child histogramDuration(final String nodeName, final String nodeType, final String schemaId, final String stage, final String attributeName, final String attributeValue) {
        return histogramDuration.labels(nodeName, nodeType, schemaId, stage, attributeName, attributeValue);
    }

    public static Histogram.Child histogramBytes(final String nodeName, final String nodeType, final String schemaId, final String stage) {
        return histogramBytes.labels(nodeName, nodeType, schemaId, stage, NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN);
    }

    public static Histogram.Child histogramBytes(final String nodeName, final String nodeType, final String schemaId, final String stage, final String attributeName, final String attributeValue) {
        return histogramBytes.labels(nodeName, nodeType, schemaId, stage, attributeName, attributeValue);
    }

    public static Histogram.Child histogramSize(final String nodeName, final String nodeType, final String schemaId, final String stage) {
        return histogramSize.labels(nodeName, nodeType, schemaId, stage, NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN);
    }

    public static Histogram.Child histogramSize(final String nodeName, final String nodeType, final String schemaId, final String stage, final String attributeName, final String attributeValue) {
        return histogramSize.labels(nodeName, nodeType, schemaId, stage, attributeName, attributeValue);
    }

    public static Gauge.Child gauge(final String nodeName, final String nodeType, final String schemaId, final String stage) {
        return gauge.labels(nodeName, nodeType, schemaId, stage, NOT_APPLICABLE_HYPHEN, NOT_APPLICABLE_HYPHEN);
    }

    public static Gauge.Child gauge(final String nodeName, final String nodeType, final String schemaId, final String stage, final String attributeName, final String attributeValue) {
        return gauge.labels(nodeName, nodeType, schemaId, stage, attributeName, attributeValue);
    }

    public static Counter.Child exprCounter(final String nodeName, final String nodeType, final String schemaId, final String attribute) {
        Pair<String, String> pair = metricAttributePair(attribute);
        return exprCounter(nodeName, nodeType, schemaId, pair.getKey(), pair.getValue());
    }

    private static Pair<String, String> metricAttributePair(final String attribute) {
        int index = attribute.indexOf(METRIC_ATTRIBUTE_DELIMITER);
        String attributeName;
        String attributeValue;
        if (index < 0) {
            attributeName = attribute;
            attributeValue = NOT_APPLICABLE_HYPHEN;
        } else if (index == 0) {
            attributeName = NOT_APPLICABLE_HYPHEN;
            attributeValue = attribute.substring(METRIC_ATTRIBUTE_DELIMITER.length());
        } else {
            attributeName = attribute.substring(0, index);
            attributeValue = attribute.substring(index + METRIC_ATTRIBUTE_DELIMITER.length());
        }
        return Pair.of(attributeName, attributeValue);
    }

    public static Counter.Child exprCounter(final String nodeName, final String nodeType, final String schemaId, final String attributeName, final String attributeValue) {
        return exprCounter.labels(nodeName, nodeType, schemaId, EXPR, attributeName, attributeValue);
    }

    public static Histogram.Child exprHistogram(final String nodeName, final String nodeType, final String schemaId, final String attribute) {
        Pair<String, String> pair = metricAttributePair(attribute);
        return exprHistogram(nodeName, nodeType, schemaId, pair.getKey(), pair.getValue());
    }

    public static Histogram.Child exprHistogram(final String nodeName, final String nodeType, final String schemaId, final String attributeName, final String attributeValue) {
        return exprHistogram.labels(nodeName, nodeType, schemaId, EXPR, attributeName, attributeValue);
    }

    public static Gauge.Child exprGauge(final String nodeName, final String nodeType, final String schemaId, final String attribute) {
        Pair<String, String> pair = metricAttributePair(attribute);
        return exprGauge(nodeName, nodeType, schemaId, pair.getKey(), pair.getValue());
    }

    public static Gauge.Child exprGauge(final String nodeName, final String nodeType, final String schemaId, final String attributeName, final String attributeValue) {
        return exprGauge.labels(nodeName, nodeType, schemaId, EXPR, attributeName, attributeValue);
    }
}
