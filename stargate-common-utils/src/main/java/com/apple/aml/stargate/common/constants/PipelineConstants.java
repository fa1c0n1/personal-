package com.apple.aml.stargate.common.constants;

import com.apple.aml.stargate.common.configs.EnvironmentConfig;
import com.apple.aml.stargate.common.configs.NodeConfig;
import com.apple.aml.stargate.common.configs.RunnerConfig;
import com.apple.aml.stargate.common.configs.VersionInfo;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.STARGATE;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.jvm.commons.util.Strings.isBlank;

public interface PipelineConstants {
    String HEADER_PIPELINE_TOKEN = "X-Pipeline-Token";
    String HEADER_PIPELINE_APP_ID = "X-Pipeline-App-ID";
    String HEADER_PIPELINE_SHARED_TOKEN = "X-Pipeline-Shared-Token";
    String HEADER_PIPELINE_SCHEMA_ID = "X-Schema-Id";
    String HEADER_PIPELINE_PUBLISHER_ID = "X-Kafka-PublisherId";
    String HEADER_PIPELINE_PAYLOAD_KEY = "X-Payload-Key";
    String HEADER_PIPELINE_PUBLISH_FORMAT = "X-Publish-Format";
    String HEADER_SURFACE_QUEUE = "X-Surface-Queue";
    String HEADER_SURFACE_BASE_PATH = "X-Surface-Base-Path";
    String CC_PIPELINE_CONFIG_SUFFIX = "-pipeline-config";
    String CC_SYSTEM_CONNECT_SUFFIX = "-system-connect";
    String PROPERTY_PIPELINE_TOKEN = "pipeline.token";
    String PROPERTY_PIPELINE_APP_ID = "pipeline.appId";
    String MODEL_PIPELINE_CLIENT_ID_SUFFIX = "-external-client";
    String CC_PUBLISHER = "publishers";
    String CC_DEFAULT_PIPELINE = "default-settings";
    String PIPELINE_ID = "pipelineId";
    String DEFAULT_MAPPING = "DEFAULT";
    String DEPLOYMENT_RESOURCE_NAME = "deployment.json";
    VersionInfo VERSION_INFO = ConfigBeanFactory.create(AppConfig.config().getConfig("stargate.versionInfo"), VersionInfo.class);
    String RUNTIME_LOGGER_NODE_PREFIX = "stargate.runtime.node";
    String SURFACE_COMPONENT = "component";
    String SURFACE_COMPONENT_JOBMANAGER = "jobmanager";
    String SURFACE_DEPLOYMENT_NAME = "deploymentName";
    String SURFACE_DEPLOYMENT_APP = "app";

    enum ENVIRONMENT {
        PROD, UAT, QA, DEV, LOCAL;

        static {
            Config envConfig = AppConfig.config().getConfig(EnvironmentConfig.CONFIG_NAME);
            for (final String key : envConfig.root().keySet()) {
                ENVIRONMENT env = ENVIRONMENT.valueOf(key.toUpperCase());
                env.config = ConfigBeanFactory.create(envConfig.getConfig(key), EnvironmentConfig.class);
                try {
                    env.config.init(env.name());
                } catch (Exception e) {
                    throw new IllegalArgumentException();
                }
            }
        }

        private EnvironmentConfig config = new EnvironmentConfig();

        public static ENVIRONMENT environment(final String input) {
            ENVIRONMENT environment = isBlank(input) ? null : ENVIRONMENT.valueOf(input.toUpperCase());
            if (environment != null) {
                return environment;
            }
            return ENVIRONMENT.PROD;
        }

        public EnvironmentConfig getConfig() {
            return config;
        }

    }

    enum CATALOG_TYPE {
        hive("org.apache.iceberg.hive.HiveCatalog"), jdbc("org.apache.iceberg.jdbc.JdbcCatalog"), glue("org.apache.iceberg.aws.glue.GlueCatalog"), dynamodb("org.apache.iceberg.aws.glue.GlueCatalog");

        private final String implementationClassName;

        CATALOG_TYPE(final String implementationClassName) {
            this.implementationClassName = implementationClassName;
        }

        public String catalogImpl() {
            return implementationClassName;
        }
    }

    enum NODE_TYPE {
        Switch, ACICassandra, ACIKafka, Attributes, AttributesOperations, BatchFunction, Cassandra, DataGenerator, Dhari, DhariSDKClient, EnhanceRecord, FieldExtractor, ErrorNode, ExternalBatchFunction, ExternalFunction, FileSystem, FreemarkerEvaluator, FreemarkerFunction, GetState, Hdfs, HdfsOperations, Http, Iceberg, IcebergOperations, IcebergSparkQueryExecutor, JavaBiFunction, JavaFunction, Jdbc, JdbcOperations, LocalOfs, Log, NoOp, PredicateFilter, Python, S3, SaveState, Scala, Sequencer, Snowflake, SnowflakeOperations, Solr, SPELEvaluator, Splunk, SubsetRecord, Stratos, BeamSQL, MetricNode, FlinkSQL;
        private static final ConcurrentHashMap<String, NODE_TYPE> LOOKUP = new ConcurrentHashMap<>();

        static {
            Config nodeConfig = AppConfig.config().getConfig(NodeConfig.CONFIG_NAME);
            for (final String key : nodeConfig.root().keySet()) {
                NODE_TYPE nodeType = NODE_TYPE.valueOf(key);
                nodeType.config = ConfigBeanFactory.create(nodeConfig.getConfig(key), NodeConfig.class);
                if (nodeType.config.getAliases() != null && !nodeType.config.getAliases().isEmpty()) {
                    for (final String alias : nodeType.config.getAliases()) {
                        nodeType.aliases.add(alias.toLowerCase());
                        nodeType.aliases.add(nodeType.name().toLowerCase());
                    }
                }
                try {
                    nodeType.config.init();
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }

        private final Set<String> aliases = new HashSet<>();
        private NodeConfig config = new NodeConfig();

        public static NODE_TYPE nodeType(final String input) {
            if (isBlank(input)) return null;
            String key = input.toLowerCase();
            NODE_TYPE value = LOOKUP.computeIfAbsent(key, x -> {
                for (NODE_TYPE type : NODE_TYPE.values()) {
                    if (type.name().equals(key) || type.aliases.contains(key)) return type;
                }
                return null;
            });
            return value;
        }

        public NodeConfig getConfig() {
            return config;
        }
    }

    enum DATA_FORMAT {
        csv, xlsx, avro, parquet, json, txt, xsv, yml, properties, conf, raw, bytearray, bytearraystring
    }

    enum EXTERNAL_FUNC {
        python(8878, 8978), jvm(8868, 8968), generic(8818, 8918);
        private int portNo;
        private int debugPortNo;

        EXTERNAL_FUNC(int portNo, int debugPortNo) {
            this.portNo = portNo;
            this.debugPortNo = debugPortNo;
        }

        public static EXTERNAL_FUNC funcType(final String runtimeType, final String nodeType) {
            try {
                EXTERNAL_FUNC type = EXTERNAL_FUNC.valueOf(String.valueOf(runtimeType).toLowerCase());
                if (type != null) return type;
            } catch (Exception ignored) {

            }
            if (nodeType.toLowerCase().contains("python") || nodeType.toLowerCase().contains("jinja")) return python;
            if (nodeType.toLowerCase().contains("jvm")) return python;
            return generic;
        }

        public int portNo() {
            return this.portNo;
        }

        public int debugPortNo() {
            return this.debugPortNo;
        }
    }

    enum APPLE_SCHEMA_STORE_REFERENCE {
        PROD(""), UAT5("-qa-itms5"), UAT6("-qa-itms6"), UAT7("-qa-itms7"), UAT8("-qa-itms8"), QA5("-qa-itms5"), QA6("-qa-itms6"), QA7("-qa-itms7"), QA8("-qa-itms8");
        private final String endpoint;

        APPLE_SCHEMA_STORE_REFERENCE(final String key) {
            this.endpoint = "schemastore" + key + ".itunes.apple.com";
        }

        public String endpoint() {
            return endpoint;
        }

    }

    enum SCHEMA_REFERENCE_TYPE {
        AppleSchemaStore("apple-schema-store"), ConfluentSchemaRegistry("confluent-schema-registry");

        private static final ConcurrentHashMap<String, SCHEMA_REFERENCE_TYPE> LOOKUP = new ConcurrentHashMap<>();

        private final Set<String> aliases;

        SCHEMA_REFERENCE_TYPE(final String... aliases) {
            this.aliases = new HashSet<>();
            for (final String alias : aliases) {
                this.aliases.add(alias.toLowerCase());
                this.aliases.add(alias.replace('-', '_').toLowerCase());
                this.aliases.add(this.name().toLowerCase());
            }
        }

        public static SCHEMA_REFERENCE_TYPE schemaReferenceType(final String input) {
            if (isBlank(input)) {
                return AppleSchemaStore;
            }
            String key = input.toLowerCase();
            SCHEMA_REFERENCE_TYPE value = LOOKUP.computeIfAbsent(key, x -> {
                for (SCHEMA_REFERENCE_TYPE type : SCHEMA_REFERENCE_TYPE.values()) {
                    if (type.aliases.contains(key)) {
                        return type;
                    }
                }
                return null;
            });
            if (value == null) {
                return AppleSchemaStore;
            }
            return value;
        }
    }

    enum PARTITION_STRATEGY {
        hash, random, roundrobin, vmroundrobin, hostrandom, hostroundrobin, host, pipeline, nullkey
    }

    enum BATCH_WRITER_CLOSE_TYPE {
        windowTimer, batchTimer, batchSize, batchBytes, finishBundle, tearDown, retryPrevious
    }

    enum RUNNER {
        direct, spark, driver, flink , nativeflink;

        static {
            Config cfg = AppConfig.config().getConfig(RunnerConfig.CONFIG_NAME);
            for (final String key : cfg.root().keySet()) {
                RUNNER runner = RUNNER.valueOf(key.toLowerCase());
                runner.config = ConfigBeanFactory.create(cfg.getConfig(key), RunnerConfig.class);
                try {
                    runner.config.init();
                } catch (Exception e) {
                    throw new IllegalArgumentException();
                }
            }
        }

        private RunnerConfig config = new RunnerConfig();

        public static RUNNER runner(final String input) {
            RUNNER runner = isBlank(input) ? null : RUNNER.valueOf(input.toLowerCase());
            if (runner != null) {
                return runner;
            }
            return RUNNER.flink;
        }

        public RunnerConfig getConfig() {
            return config;
        }

    }

    enum DEPLOYMENT_TARGET {
        stargate, surface
    }

    enum DEPLOYMENT_SIZE {
        xs("x-small", "extra-small"), s("small"), m("medium"), l("large"), xl("x-large", "extra-large"), xxl("double-x-large", "double-large", "double-xl"), xxxl("triple-x-large", "triple-large", "triple-xl"), custom("custom", "user-defined", "manual");
        private static final ConcurrentHashMap<String, DEPLOYMENT_SIZE> LOOKUP = new ConcurrentHashMap<>();
        private final Set<String> aliases = new HashSet<>();

        DEPLOYMENT_SIZE(final String... strings) {
            aliases.add(this.name());
            for (String str : strings) {
                aliases.add(str);
                aliases.add(str.replace("-", ""));
                aliases.add(str.replace("-", "_"));
            }
        }

        public static DEPLOYMENT_SIZE deploymentSize(final String input) {
            if (isBlank(input)) {
                return DEPLOYMENT_SIZE.s;
            }
            String key = input.toLowerCase();
            DEPLOYMENT_SIZE value = LOOKUP.computeIfAbsent(key, x -> {
                for (DEPLOYMENT_SIZE type : DEPLOYMENT_SIZE.values()) {
                    if (type.name().equals(key) || type.aliases.contains(key)) {
                        return type;
                    }
                }
                return custom;
            });
            return value;
        }
    }

    enum SANDBOX_COMPONENT {
        MINIO("minio");

        private final String component;

        SANDBOX_COMPONENT(final String component) {
            this.component = component;
        }

        public String getComponent() {
            return component;
        }
    }

    interface EnvironmentVariables {

        String APP_LOG_OVERRIDES_FILE_PATH = "APP_LOG_OVERRIDES_FILE_PATH";
        String APP_ON_EXECUTION_ACTION = "APP_ON_EXECUTION_ACTION";
        String STARGATE_BASE_SHARED_DIRECTORY = "STARGATE_BASE_SHARED_DIRECTORY";
        String STARGATE_SHARED_DIRECTORY = "STARGATE_SHARED_DIRECTORY";
        String STARGATE_ADDITIONAL_HADOOP_CONF_DIR = "STARGATE_ADDITIONAL_HADOOP_CONF_DIR";
        String STARGATE_PIPELINE_ID = "STARGATE_PIPELINE_ID";
        String STARGATE_SHARED_TOKEN = "STARGATE_SHARED_TOKEN";
        String APP_LOG_ADDITIONAL_PREFIX = "APP_LOG_ADDITIONAL_PREFIX";
        String STARGATE_HUBBLE_PUBLISH_TOKEN = "STARGATE_HUBBLE_PUBLISH_TOKEN";
        String STARGATE_HUBBLE_METRICS_ENABLED = "STARGATE_HUBBLE_METRICS_ENABLED";
        String STARGATE_HDFS_SHARED_CONFIG_FILE = "hdfs-config.json";
        String STARGATE_ENABLE_DYNAMIC_KERBEROS_LOGIN = "STARGATE_ENABLE_DYNAMIC_KERBEROS_LOGIN";
        String STARGATE_ENABLE_INSECURE_HMS = "STARGATE_ENABLE_INSECURE_HMS";
        String STARGATE_REMOTE_RM_URI = "STARGATE_REMOTE_RM_URI";
        String STARGATE_LOCAL_RM_URI = "STARGATE_LOCAL_RM_URI";
        String STARGATE_CONF = ".conf";
        String STARGATE_KEYTAB = ".keytab";
        String STARGATE_OPERATOR_URI = "STARGATE_OPERATOR_URI";
        String STARGATE_CONNECT_BACKEND = "STARGATE_CONNECT_BACKEND";
        String STARGATE_EXTERNAL_FUNC_ENDPOINTS = "STARGATE_EXTERNAL_FUNC_ENDPOINTS";
        String STARGATE_CONTAINER_PORT = "STARGATE_CONTAINER_PORT";
        String STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE = "STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE";
        String STARGATE_CONTAINER_DEBUG_PORT = "STARGATE_CONTAINER_DEBUG_PORT";
        String STARGATE_MAIN_CONTAINER_GRPC_ENDPOINT = "STARGATE_MAIN_CONTAINER_GRPC_ENDPOINT";
        String STARGATE_OPERATOR_JOB_DELETE_WAIT_TIME = "STARGATE_OPERATOR_JOB_DELETE_WAIT_TIME";
        String APPCONFIG_MODULE_ID = "APPCONFIG_MODULE_ID";
        String APPCONFIG_ENVIRONMENT = "APPCONFIG_ENVIRONMENT";

        static String sharedToken() {
            String token = env(STARGATE_SHARED_TOKEN, null);
            return token == null ? pipelineId() : token;
        }

        static String pipelineId() {
            String id = env(STARGATE_PIPELINE_ID, null);
            return id == null ? env("APP_PIPELINE_ID", null) : id;
        }
    }

    interface DirectoryPaths {
        String STARGATE_HOME = "/opt/stargate";
        String FLINK_HOME = "/opt/flink";
        String SPARK_HOME = "/opt/spark";
        String FLINK_CONF_FILE = String.format("%s/conf/flink-conf.yaml", FLINK_HOME);
        String STARGATE_LOGS = String.format("%s/logs", STARGATE_HOME);
        String APP_ROOT_PATH = "/app";
        String EXTERNAL_STARGATE_HOME = String.format("%s/%s", APP_ROOT_PATH, STARGATE);
        String EXTERNAL_APP_LIB = String.format("%s/lib", APP_ROOT_PATH);
        String EXTERNAL_APP_CONF = String.format("%s/conf", APP_ROOT_PATH);
        String CONNECT_ID_PROP_FILE_NAME = "properties";
        String STARGATE_RUNTIME_HOME = "/opt/stargate-runtime";
    }

    interface Krb5Properties {
        String AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
        String KRB5_DEBUG = "sun.security.krb5.debug";
        String DEFAULT_FILENAME_KERBEROS_KEYTAB = "kerberos.keytab";
        String DEFAULT_FILENAME_KERBEROS_KRB5 = "krb5.conf";
    }

    interface SparkProperties {
        String SPARK_PROP_PREFIX = "spark.";
        String STARGATE_SPARK_PROP_PREFIX = SPARK_PROP_PREFIX + STARGATE + ".";
        String STARGATE_SPARK_PROP_SHARED_DIR_PATH = STARGATE_SPARK_PROP_PREFIX + "shared.dir";
        String STARGATE_SPARK_PROP_SHARED_FILES = STARGATE_SPARK_PROP_PREFIX + "shared.files";
        String STARGATE_SPARK_PROP_SHARED_FILES_COPY = STARGATE_SPARK_PROP_PREFIX + "shared.files.copy";
        String STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL = STARGATE_SPARK_PROP_PREFIX + "kerberos.principal";
        String STARGATE_SPARK_PROP_KERBEROS_KEYTAB = STARGATE_SPARK_PROP_PREFIX + Krb5Properties.DEFAULT_FILENAME_KERBEROS_KEYTAB;
        String STARGATE_SPARK_PROP_KERBEROS_KRB5 = STARGATE_SPARK_PROP_PREFIX + Krb5Properties.DEFAULT_FILENAME_KERBEROS_KRB5;
        String STARGATE_SPARK_EXECUTOR_ENV_PREFIX = STARGATE_SPARK_PROP_PREFIX + "executor.env.";
        String STARGATE_SPARK_EXECUTOR_SYS_PREFIX = STARGATE_SPARK_PROP_PREFIX + "executor.sys.";
        String STARGATE_SPARK_EXECUTOR_PROPERTIES_FILE = "executor.properties";
        String SPARK_HADOOP_PROP_PREFIX = SPARK_PROP_PREFIX + "hadoop.";
        String SPARK_K8S_HADOOP_CONF_DIR_PATH = "HADOOP_CONF_DIR";
        String SPARK_CHECKPOINT_AUTHORITY = "checkpoint";
    }


}
