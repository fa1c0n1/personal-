package org.apache.calcite.adapter.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoDatabase;
import org.apache.calcite.schema.Table;

import java.util.Map;

public class MongoDbSchema extends MongoSchema {
    private final MongoDatabase database;

    public MongoDbSchema(final MongoDatabase database) {
        super(database);
        this.database = database;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (final String collectionName : database.listCollectionNames()) {
            builder.put(collectionName, new MongoTable(collectionName));
        }
        return builder.build();
    }
}
