package com.apple.aml.stargate.common.constants;

import com.apple.amp.schemastore.model.SchemaStoreEndpoint;
import com.apple.pie.queue.client.ext.schema.header.SchemaHeaderSerDe;
import com.apple.pie.queue.kafka.client.kaffe.KaffeConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Map;

import static com.apple.aml.stargate.common.utils.SchemaUtils.appleSchemaStoreEndpoint;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_HOST_CONFIG;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_PORT_CONFIG;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_PROTOCOL_CONFIG;
import static com.apple.pie.queue.kafka.envelope.EnvelopeConfig.addExtensionSerdesToConfig;

public interface KafkaConstants {
    String KAFFE_CONFIG_PREFIX = KaffeConfig.CONFIG_PREFIX;
    String PUBLISHER_ID = "publisherId";
    String CONSUMER_ID = "consumerId";
    String TRANSACTION_ID = "txnId";
    String GROUP = "group";
    String NAMESPACE = "namespace";
    String TOPIC = CommonConstants.MetricLabels.TOPIC;
    String PARTITION = "partition";
    String RECORD_KEY = "key";
    String OFFSET = "offset";
    String CLIENT_ID = "clientId";
    String SCHEMA_ID = CommonConstants.SCHEMA_ID;
    String UNSTRUCTURED_FIELD_NAME = "unstructuredFieldName";
    String KAFKA_TIMESTAMP = "kafkaTimeStamp";
    String SERIALIZED_KEY_SIZE = "serializedKeySize";
    String SERIALIZED_VALUE_SIZE = "serializedValueSize";
    String DEFAULT_APP_TYPE = "app";
    String ACI_FORMAT_HEADER_NAME = "pie.fmt";

    static Map<String, Object> appendSchemaStoreEndpointDetails(final Map<String, Object> configs, final String input) {
        SchemaStoreEndpoint schemaStoreEndpoint = appleSchemaStoreEndpoint(input);
        configs.put(SCHEMA_STORE_PROTOCOL_CONFIG, schemaStoreEndpoint.getProtocol());
        configs.put(SCHEMA_STORE_HOST_CONFIG, schemaStoreEndpoint.getHost());
        configs.put(SCHEMA_STORE_PORT_CONFIG, schemaStoreEndpoint.getPort());
        addExtensionSerdesToConfig(configs, SchemaHeaderSerDe.class);
        return configs;
    }

    static RecordHeaders getCompatibleACIHeaders() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(ACI_FORMAT_HEADER_NAME, new byte[]{1}));
        return headers;
    }

    enum BEAM_TIME_POLICY {
        processingtime, logappendtime, createtime
    }

    enum EAI_DHARI_FIELDS {
        DHARI_TIME_STAMP, DHARI_TRANSACTION_ID, DHARI_TABLE_NAME, DHARI_TABLE_TYPE, DHARI_PAYLOAD_SCHEMA_ID, DHARI_PAYLOAD_TOPIC, DHARI_EVENT_TYPE, DHARI_RECORD_COUNT, EAI_FILE_DIRECTORY, EAI_FILE_NAME, EAI_FILE_HEADER, EAI_FILE_TIME_STAMP, EAI_FILE_COLUMN_HEADER, EAI_FILE_INTERFACE, EAI_FILE_RECORD_COUNT, DHARI_PAYLOAD_PARTITION_COUNT, DHARI_METADATA_SCHEMA_ID, DHARI_METADATA_TOPIC, DHARI_METADATA_PARTITION_COUNT, EAI_REQ_FILE_UUID, EAI_REQ_FILE_NAME, EAI_REQ_INSTANCE_NAME, EAI_REQ_PUB_APP_NAME, EAI_REQ_SUB_APP_NAME, EAI_REQ_EVENT_NAME, EAI_REQ_INTERFACE_NAME, EAI_REQ_TIME_STAMP, EAI_REQ_TABLE_NAME, EAI_REQ_TABLE_TIME
    }

    enum S3_DHARI_FIELDS {
        S3_ENABLED, S3_BUCKET, S3_OBJECT_PATH, S3_OBJECT_FILES, S3_OBJECT_FILE, S3_OBJECT_SIZE
    }

    interface EaiHeaders {
        String EAI_INSTANCE_NAME = "EAI_INSTANCE_NAME";
        String EAI_INTERFACE_NAME = "EAI_INTERFACE_NAME";
        String EAI_EVENT_NAME = "EAI_EVENT_NAME";
        String EAI_A2A_TOKEN = "EAI_A2A_TOKEN";
        String EAI_MSG_GUID = "EAI_MSG_GUID";
    }

    interface EaiDhariConstants {
        String EAI_PROXY_APP_ID = "EAI_PROXY_APP_ID";
        String EAI_PROXY_TARGET_APP_ID = "EAI_PROXY_TARGET_APP_ID";
        String EVENT_TYPE_START = "START";
        String EVENT_TYPE_END = "END";
        char EAI_XTFILE_DELIMITER = '^';
    }
}
