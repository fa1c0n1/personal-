package org.apache.calcite.adapter.mongodb;

import com.mongodb.client.MongoDatabase;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MongoDbSchemaFactory implements SchemaFactory {
    public static MongoDbSchemaFactory INSTANCE;
    private final MongoDatabase database;

    public MongoDbSchemaFactory(final MongoDatabase database) {
        this.database = database;
    }

    @Override
    public Schema create(final SchemaPlus parentSchema, final String name, final Map<String, Object> operand) {
        return new MongoDbSchema(this.database);
    }
}
