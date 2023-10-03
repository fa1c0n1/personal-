package com.apple.aml.stargate.runners.spark.utils;

import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.utils.AppConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APP_LOG_ADDITIONAL_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_HUBBLE_METRICS_ENABLED;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_HUBBLE_PUBLISH_TOKEN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_SHARED_DIRECTORY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.SPARK_HADOOP_PROP_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.SPARK_K8S_HADOOP_CONF_DIR_PATH;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.SPARK_PROP_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_EXECUTOR_ENV_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_KEYTAB;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_SHARED_DIR_PATH;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class SparkUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final AtomicReference<SparkConf> SPARK_RUNTIME_CONF = new AtomicReference<>();
    private static final AtomicReference<SparkConf> ADDITIONAL_SPARK_CONF_REF = new AtomicReference<>();
    private static final AtomicReference<SparkSession> SPARK_SESSION = new AtomicReference<>();
    private static boolean sparkEnabled; // TODO : Need some other optimized way to figure this out ( should not use Reflection either )

    private SparkUtils() {

    }

    public static void initSparkSession() {
        LOGGER.debug("Initializing Spark Session");
        SparkSession.builder().config(sparkConf()).appName(AppConfig.appName().toLowerCase()).getOrCreate();
        SPARK_SESSION.set(SparkSession.active());
        LOGGER.debug("Spark Session initialized successfully");
    }

    public static SparkConf sparkConf() {
        SparkConf conf = SPARK_RUNTIME_CONF.get();
        if (conf == null) {
            return getAdditionalSparkConf();
        }
        return conf;
    }

    public static SparkConf getAdditionalSparkConf() {
        return ADDITIONAL_SPARK_CONF_REF.get();
    }

    @SuppressWarnings("unchecked")
    public static void initProperties(final StargateOptions options) throws Exception {
        PipelineConstants.RUNNER runner = PipelineConstants.RUNNER.valueOf(isBlank(options.getPipelineRunner()) ? PipelineConstants.RUNNER.direct.name() : options.getPipelineRunner());
        if (!(runner == PipelineConstants.RUNNER.spark || runner == PipelineConstants.RUNNER.driver)) {
            return;
        }
        sparkEnabled = true;
        Map<String, String> sparkProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            System.setProperty(key, value);
            if (key.startsWith(SPARK_HADOOP_PROP_PREFIX)) {
                key = key.substring(SPARK_HADOOP_PROP_PREFIX.length());
                System.setProperty(key, value);
            }
        }
        SparkConf sparkConf = getSparkConf();
        String logLine = ",pipelineId=\"" + pipelineId() + "\"";
        String hubblePublishKey = env(STARGATE_HUBBLE_PUBLISH_TOKEN, null);
        if (hubblePublishKey != null && hubblePublishKey.trim().isBlank()) {
            hubblePublishKey = null;
        }
        if (sparkConf != null) {
            for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
                sparkConf.set(entry.getKey(), entry.getValue());
            }
            sparkConf.set(STARGATE_SPARK_PROP_SHARED_DIR_PATH, options.getSharedDirectoryPath());
            sparkConf.setExecutorEnv(STARGATE_SHARED_DIRECTORY, options.getSharedDirectoryPath());
            sparkConf.setExecutorEnv(SPARK_K8S_HADOOP_CONF_DIR_PATH, options.getSharedDirectoryPath());
            sparkConf.setExecutorEnv(STARGATE_PIPELINE_ID, pipelineId());
            sparkConf.setExecutorEnv(APP_LOG_ADDITIONAL_PREFIX, logLine);
            if (hubblePublishKey != null) {
                sparkConf.setExecutorEnv(STARGATE_HUBBLE_PUBLISH_TOKEN, hubblePublishKey);
            }
        }
        System.setProperty(STARGATE_SHARED_DIRECTORY, options.getSharedDirectoryPath());
        System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + STARGATE_PIPELINE_ID, pipelineId());
        System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + APP_LOG_ADDITIONAL_PREFIX, logLine);
        System.setProperty(STARGATE_SPARK_PROP_SHARED_DIR_PATH, options.getSharedDirectoryPath());
        System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + SPARK_K8S_HADOOP_CONF_DIR_PATH, options.getSharedDirectoryPath());
        System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + STARGATE_SHARED_DIRECTORY, options.getSharedDirectoryPath());
        if (hubblePublishKey != null) {
            sparkConf.setExecutorEnv(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + STARGATE_HUBBLE_PUBLISH_TOKEN, hubblePublishKey);
        }
        sparkConf.setExecutorEnv(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + STARGATE_HUBBLE_METRICS_ENABLED, String.valueOf(parseBoolean(env(STARGATE_HUBBLE_METRICS_ENABLED, "false"))));
        Configuration conf = getSparkHadoopConfig();
        if (conf != null) {
            for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
                String key = String.valueOf(entry.getKey());
                String value = String.valueOf(entry.getValue());
                if (key.startsWith(SPARK_HADOOP_PROP_PREFIX)) {
                    key = key.substring(SPARK_HADOOP_PROP_PREFIX.length());
                }
                conf.set(key, value);
            }
        }
    }

    public static SparkConf getSparkConf() {
        SparkConf current = ADDITIONAL_SPARK_CONF_REF.get();
        if (current != null) {
            return current;
        }
        ADDITIONAL_SPARK_CONF_REF.compareAndSet(null, new SparkConf(true));
        return ADDITIONAL_SPARK_CONF_REF.get();
    }

    public static Configuration getSparkHadoopConfig() {
        if (SparkHadoopUtil.get() == null) {
            return null;
        }
        return SparkHadoopUtil.get().conf();
    }

    @SuppressWarnings("unchecked")
    public static void appendSparkHadoopProperties(final Map<String, String> properties, final List<String> resources) {
        if (!sparkEnabled) {
            return;
        }
        //System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + "PSPRK_PIE_INITIALIZE_KERBEROS", "true");
        if (properties.containsKey(STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL) && properties.containsKey(STARGATE_SPARK_PROP_KERBEROS_KEYTAB)) {
            String principal = properties.get(STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL);
            System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + "PLATFORM_KERBEROS_PRINCIPAL", principal);
            System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + "SPARK_USER", principal);
            //System.setProperty(STARGATE_SPARK_EXECUTOR_ENV_PREFIX + "PSPRK_PIE_KERBEROS_KEYTAB_PATH", properties.get(STARGATE_SPARK_PROP_HDFS_KEYTAB));
        }
        properties.keySet().stream().filter(x -> x.startsWith(STARGATE_SPARK_PROP_PREFIX) || x.startsWith(SPARK_PROP_PREFIX)).forEach(x -> System.setProperty(x, properties.get(x)));
        SparkConf sparkConf = getSparkConf();
        if (sparkConf != null) {
            properties.forEach((k, v) -> sparkConf.set(String.valueOf(k), String.valueOf(v)));
        }
    }

    public static void setSparkRuntimeConf(final SparkConf sparkConf) {
        if (sparkConf != null) {
            SPARK_RUNTIME_CONF.set(sparkConf);
        }
    }

    public static SparkSession spark() {
        SparkSession session = SPARK_SESSION.get();
        if (session == null) {
            session = SparkSession.active();
        }
        return session;
    }
}
