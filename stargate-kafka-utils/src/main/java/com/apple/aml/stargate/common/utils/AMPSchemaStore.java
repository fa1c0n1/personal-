package com.apple.aml.stargate.common.utils;

import com.apple.amp.schemastore.constants.SchemaStoreConstants;
import com.apple.pie.queue.client.ext.schema.avro.VersionedSchema;
import com.apple.pie.queue.client.ext.schema.avro.store.AvroSchemaStore;
import com.apple.pie.queue.client.ext.schema.avro.store.apple.AppleAvroSchemaStore;
import com.apple.pie.queue.client.ext.schema.header.SchemaDetails;
import org.apache.avro.Schema;
import org.apache.kafka.common.Configurable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AMPSchemaStore implements AvroSchemaStore, Configurable {
    private static final ConcurrentHashMap<String, Schema> CACHE = new ConcurrentHashMap<>(); // TODO : We need to use something like Cache2k with some TTL
    private final AppleAvroSchemaStore store = new AppleAvroSchemaStore();

    @Override
    public SchemaDetails insertSchema(final Schema schema) {
        return store.insertSchema(schema);
    }

    @Override
    public SchemaDetails retrieveSchemaDetails(final Schema schema) {
        return store.retrieveSchemaDetails(schema);
    }

    @Override
    public Schema retrieveSchema(final SchemaDetails details) {
        String cacheKey = String.format("%s:%d:%d", details.getSchemaName(), (int) details.getSchemaVersion(), SchemaStoreConstants.INITIAL_REVISION);
        Schema schema = CACHE.computeIfAbsent(cacheKey, key -> store.retrieveSchema(details));
        if (schema == null) {
            schema = store.retrieveSchema(details);
            CACHE.put(cacheKey, schema);
        }
        return schema;
    }

    @Override
    public VersionedSchema retrieveLatestSchema(final String schemaName) {
        return store.retrieveLatestSchema(schemaName);
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        store.configure(configs);
    }
}
