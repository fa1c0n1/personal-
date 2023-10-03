package com.apple.aml.stargate.common.constants;

import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigSyntax;
import okhttp3.MediaType;
import org.apache.commons.lang3.time.FastDateFormat;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public interface CommonConstants {
    String STARGATE = "stargate";
    String DOT = ".";
    String EMPTY_STRING = "";
    String DEFAULT_DELIMITER = ",";
    char DEFAULT_DELIMITER_CHAR = ',';
    String SPECIAL_DELIMITER_CAP = "^";
    String METRIC_DELIMITER = "_";
    String METRIC_ATTRIBUTE_DELIMITER = "::";
    String SCHEMA_DELIMITER = ":";
    String SPECIAL_DELIMITER_CAP_REGEX = "\\" + SPECIAL_DELIMITER_CAP;
    String REDACTED_STRING = "****redacted****";
    String HEADER_A3_TOKEN = "X-Apple-IDMS-A3-Token";
    String HEADER_CLIENT_APP_ID = "X-Apple-Client-App-ID";
    String HEADER_TOKEN_TYPE = "X-Token-Type";
    String HEADER_RM_HOST = "X-Stargate-ResourceManager-Host";
    String HEADER_RM_TOKEN = "X-Stargate-ResourceManager-Token";
    String HEADER_RM_INITIALIZATION_VECTOR = "X-Stargate-ResourceManager-Initialization-Vector";
    MediaType MEDIA_TYPE_APPLICATION_JSON = MediaType.get("application/json");
    MediaType MEDIA_TYPE_TEXT_JSON = MediaType.get(MEDIA_TYPE_APPLICATION_JSON + "; charset=utf-8");
    MediaType MEDIA_TYPE_MARKDOWN = MediaType.parse("text/x-markdown; charset=utf-8");
    MediaType MEDIA_TYPE_BINARY = MediaType.parse("application/binary; charset=utf-8");
    String MEDIA_TYPE_APPLICATION_JSON_VALUE = MEDIA_TYPE_APPLICATION_JSON.toString();
    MediaType MEDIA_TYPE_UTF_8 = MediaType.parse("charset=utf-8");
    DateTimeFormatter FORMATTER_DATE_1 = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.from(ZoneOffset.UTC));
    DateTimeFormatter FORMATTER_DATE_2 = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.from(ZoneOffset.UTC));
    DateTimeFormatter FORMATTER_DATE_TIME_1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneId.from(ZoneOffset.UTC));
    DateTimeFormatter FORMATTER_DATE_TIME_2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneId.from(ZoneOffset.UTC));
    long MAX_S3_BLOCK_SIZE = 5 * 1024 * 1024;
    long MAX_FILE_ATTACHMENT_SIZE = 1024 * 1024 * 1024;
    long MAX_BYTE_STRING_LENGTH = 10000;
    String IDMS_APP_ID = "appId";
    String ID = "id";
    String MONGO_ID = "_id";
    String API_VERSION = "v1";
    String API_PREFIX = "/api/" + API_VERSION;
    Map<String, String> SIZE_CONVERSION_H_TO_BASE2 = Map.of("k", "Ki", "m", "Mi", "g", "Gi", "t", "Ti", "p", "Pi");
    Map<String, String> SIZE_CONVERSION_BASE2_TO_H = inverseKV(SIZE_CONVERSION_H_TO_BASE2);
    List<String> DATE_TIME_PARSE_FORMATS = asList("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyyMMddHHmmssSSS", "yyyyMMddHHmmss", "yyyyMMdd");
    List<DateTimeFormatter> DATE_TIME_PARSERS = DATE_TIME_PARSE_FORMATS.stream().map(x -> DateTimeFormatter.ofPattern(x).withZone(ZoneId.from(ZoneOffset.UTC))).collect(Collectors.toList());
    List<FastDateFormat> FAST_DATE_PARSERS = DATE_TIME_PARSE_FORMATS.stream().map(x -> FastDateFormat.getInstance(x, TimeZone.getTimeZone(ZoneId.from(ZoneOffset.UTC)))).collect(Collectors.toList());
    ConfigRenderOptions CONFIG_RENDER_OPTIONS_CONCISE = ConfigRenderOptions.concise().setJson(false).setFormatted(true);
    ConfigParseOptions CONFIG_PARSE_OPTIONS_JSON = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON);
    String ENHANCE_RECORD_SCHEMA_SUFFIX = ".enhanced";

    int SCHEMA_LATEST_VERSION = -2;
    String SCHEMA_ID = "schemaId";
    String VERSION_NO = "versionNo";
    String NOT_APPLICABLE_HYPHEN = "-";


    private static <V, K> Map<V, K> inverseKV(final Map<K, V> kv) {
        Map<V, K> map = new HashMap<>();
        for (Map.Entry<K, V> entry : kv.entrySet()) {
            map.put(entry.getValue(), entry.getKey());
        }
        return map;
    }

    interface RsaHeaders {
        String PRIVATE_KEY_HEADER_INCLUDES = "-BEGIN PRIVATE KEY-";
        String PUBLIC_KEY_HEADER_INCLUDES = "-BEGIN PUBLIC KEY-";
        String BEGIN_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----";
        String END_PUBLIC_KEY = "-----END PUBLIC KEY-----";
        String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
        String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";
    }

    interface OfsConstants {
        String APP_IGNITE_DISCOVERY_TYPE = "APP_IGNITE_DISCOVERY_TYPE";
        String CACHE_KEY_PRELOAD_STATUS = "cache-preload-status";
        String CACHE_NAME_DEFAULT = "default";
        String CACHE_NAME_KV = "kvstore";
        String CACHE_NAME_RELATIONAL = "relationalstore";
        String CUSTOM_FIELD_DELIMITER = ":";
        String TEMPLATE_NAME_RELATIONAL = "relational";
        String PRIMARY_KEY_NAME = "LOCAL_OFS_ID";
        String DELETE_ATTRIBUTE_NAME = "SOFT_DELETE";
    }

    interface MetricLabels {
        String CACHE_PROVIDER_IGNITE = "ignite";
        String CACHE_PROVIDER_JDBC = "jdbc";
        String CACHE_TYPE = "cacheType";
        String CACHE_TYPE_KV = "kv";
        String CACHE_TYPE_RELATIONAL = "relational";
        String CAUSE = "cause";
        String EXCEPTION = "exception";
        String ERROR = "error";
        String FOUND = "found";
        String HIT = "hit";
        String LAMBDA = "lambda";
        String METHOD_NAME = "method";
        String MISS = "miss";
        String NODE_NAME = "nodeName";
        String NODE_TYPE = "nodeType";
        String NOT_FOUND = "notFound";
        String OK = "ok";
        String OVERALL = "overall";
        String PROVIDER = "provider";
        String REMOTE = "remote";
        String SCOPE = "scope";
        String SOURCE = "source";
        String STAGE = "stage";
        String ATTRIBUTE_NAME = "attributeName";
        String ATTRIBUTE_VALUE = "attributeValue";
        String TYPE = "type";
        String ATTRIBUTE = "attribute";
        String STATUS = "status";
        String TOPIC = "topic";
        String SUCCESS = "success";
        String SCHEMA_ID = CommonConstants.SCHEMA_ID;
        String VERSION_NO = CommonConstants.VERSION_NO;
        String UNKNOWN = "unknown";
        String EXPR = "expr";
        String TARGET_SCHEMA_ID = "targetSchemaId";
        String SOURCE_SCHEMA_ID = "sourceSchemaId";
        String COLLECTION_NAME = "collectionName";
        String DB_NAME = "dbName";
        String TABLE_NAME = "tableName";
        String BUCKET_NAME = "bucketName";
        String FILE_PATH = "filePath";
        String KEYSPACE = "keyspace";
        String INDEX_NAME = "indexName";
        String URL = "url";
        String CACHE_NAME = "cacheName";
        String ERROR_MESSAGE = "errorMessage";
    }

    interface MetricStages {
        String ELEMENTS_IN = "elements_in";
        String ELEMENTS_OUT = "elements_out";
        String ELEMENTS_ERROR = "elements_error";
        String ELEMENTS_NULL = "elements_null";
        String ELEMENTS_SKIPPED = "elements_skipped";
        String ELEMENTS_PASS_THROUGH = "elements_pass_through";
        String ELEMENTS_WRITTEN = "elements_written";
        String ELEMENTS_RETRIED = "elements_retried";
        String ELEMENTS_PARTITIONED = "elements_partitioned";
        String PROCESS = "process";
        String PROCESS_ERROR = "process_error";
        String PROCESS_SUCCESS = "process_success";
        String WRITE_SUCCESS = "write_success";
        String WRITE_ERROR = "write_error";
        String WRITE_RETRY = "write_retry";
        String UNBOUNDED_ERROR = "unbounded_error";
        String INGEST_SUCCESS = "ingest_success";
        String INGEST_ERROR = "ingest_error";
        String COMMIT_SUCCESS = "commit_success";
        String QUERY_EXECUTED = "query_executed";
        String SQL_SELECT_SUCCESS = "sql_select_success";
        String SQL_SELECT_ERROR = "sql_select_error";
        String SQL_EXECUTE_SUCCESS = "sql_execute_success";
        String SQL_EXECUTE_ERROR = "sql_execute_error";
        String SQL_INVOKE_SUCCESS = "sql_invoke_success";
        String SQL_INVOKE_ERROR = "sql_invoke_error";
    }

    interface K8sLabels {
        String RESOURCE = String.format("%s/resource", STARGATE);
        String PLATFORM = "platform";
        String WORKFLOW = "workflow";
        String RUNNER = "runner";
        String RUN_NO = "runNo";
        String VERSION_NO = CommonConstants.VERSION_NO;
        String MODE = "mode";
        String STARGATE_VERSION = "stargateVersion";
    }

    interface K8sLabelValues {
        String COORDINATOR = "coordinator";
        String WORKER = "worker";
    }

    interface K8sPorts {
        Integer RM = 8888;
        Integer DEBUGGER = 5005;
        Integer APM = 4000;
        Integer GRPC = 8808;

        Integer SCHEDULER_QUEUE = 8788;
    }

    interface K8sOperator {
        String NAMESPACE = "stargate-system";
        String GROUP = "stargate.aml.apple.com";
        String KIND = "StargateDeployment";
        String SINGULAR = "stargatedeployment";
        String PLURAL = "stargatedeployments";
        String SHORT_NAME_1 = "sg";
        String SHORT_NAME_2 = "sd";
        String VERSION = "v1";
        String API_VERSION = String.format("%s/%s", GROUP, VERSION);
    }

    interface MetricNames {
        String APP_JVM_TIME = "app_jvm_time";
        String PIPELINE_COUNTER = "stargate_counter";
        String PIPELINE_GAUGE = "stargate_gauge";
        String PIPELINE_HISTOGRAM = "stargate_histogram";
        String PIPELINE_HISTOGRAM_DURATION = "stargate_duration";
        String PIPELINE_HISTOGRAM_BYTES = "stargate_bytes";
        String PIPELINE_HISTOGRAM_SIZE = "stargate_size";
        String PIPELINE_EXPR_COUNTER = "stargate_expr_counter";
        String PIPELINE_EXPR_GAUGE = "stargate_expr_gauge";
        String PIPELINE_EXPR_HISTOGRAM = "stargate_expr_histogram";
    }

    interface FreemarkerNames {
        String KEY = "key";
        String RECORD = "record";
        String STATICS = "statics";
        String UTIL = "util";
        String SCHEMA = "schema";
        String LOAD_NO = "loadNo";
        String COUNTER = "counter";
        String GAUGE = "gauge";
        String HISTOGRAM = "histogram";
        String TYPE = "type";
        String METRIC = "metric";
    }

    interface SolrConstants {
        String QUERY_ALL = "*:*";
    }

    interface MongoInternalKeys {
        String PIPELINE_ID = "_pipelineId";
        String STATE_ID = "_stateId";
        String SAVED_ON = "_savedOn";
        String APP_ID = "_appId";
        String GROUP = "_group";
        String INSTANCE_ID = "_instanceId";
        String STATUS = "_status";
        String QUEUED_ON = "_queuedOn";
        String STARTED_ON = "_startedOn";
        String UPDATED_ON = "_updatedOn";
        String CREATED_ON = "_createdOn";
        String PROGRESS = "_progress";
        String DEBUG_INFO = "_debugInfo";
        String RETRY_COUNT = "_retryCount";
        String INVOKED_BY = "_invokedBy";
        String ACTIVE = "_active";
    }

    interface MDCKeys {
        String SOURCE_ID = "sourceId";
        String TRACKER_ID = "trackerId";
    }

    interface Expressions {
        String TABLE_NAME = "${schema.name}_${schema.version}";
        String COLLECTION_NAME = "${schema.name}_${schema.version}";
        String ALIAS_NAME = "${schema.name}";
        String FILE_PATH = "${schema.name}_${schema.version}";
        String PARTITION_PATH = "'/dt='yyyyMMdd'/hour='HH";
    }

    interface SerializationConstants {
        String CONFIG_NODE_NAME = "stargate.nodeName";
        String CONFIG_NODE_TYPE = "stargate.nodeType";
        String CONFIG_SCHEMA_ID = "stargate.kafka.schemaId";
        String CONFIG_SCHEMA_REFERENCE = "stargate.kafka.schemaReference";
    }

    interface EnvironmentVariables {

        String APP_AVRO_SERDE_USE_FAST_READER = "APP_AVRO_SERDE_USE_FAST_READER";
        String APP_KAFKA_AVRO_SERDE_USE_FAST_READER = "APP_KAFKA_AVRO_SERDE_USE_FAST_READER";
        String APP_AVRO_DEFAULT_CONVERTER = "APP_AVRO_DEFAULT_CONVERTER";
    }
}
