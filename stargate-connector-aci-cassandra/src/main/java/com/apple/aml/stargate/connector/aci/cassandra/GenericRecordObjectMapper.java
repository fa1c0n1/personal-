package com.apple.aml.stargate.connector.aci.cassandra;

import com.apple.aml.stargate.common.options.ACICassandraOptions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.KEYSPACE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TABLE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_SKIPPED;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

@SuppressWarnings("unchecked")
public class GenericRecordObjectMapper implements Mapper<GenericRecord>, Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private final String nodeType;
    private final Schema schema;
    private final Session session;
    private final ACICassandraOptions aciCassandraOptions;

    public GenericRecordObjectMapper(final String nodeName, final String nodeType, final Schema schema, final Session session, final ACICassandraOptions aciCassandraOptions) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.schema = schema;
        this.session = session;
        this.aciCassandraOptions = aciCassandraOptions;
    }

    private static Object fromAvroArray(final Schema schema, final Row row, final String fieldName) {
        switch (schema.getType()) {
            case INT:
                return row.getList(fieldName, Integer.class);
            case LONG:
                return row.getList(fieldName, Long.class);
            case FLOAT:
                return row.getList(fieldName, Float.class);
            case DOUBLE:
                return row.getList(fieldName, Double.class);
            case BOOLEAN:
                return row.getList(fieldName, Boolean.class);
            case STRING:
                return row.getList(fieldName, String.class);
            default:
                throw new UnsupportedOperationException(schema.getType() + " schema type not supported for avro List Conversion");
        }
    }

    private static Object fromAvroMap(final Schema schema, final Row row, final String fieldName) {

        switch (schema.getType()) {
            case INT:
                return row.getMap(fieldName, String.class, Integer.class);
            case LONG:
                return row.getMap(fieldName, String.class, Long.class);
            case FLOAT:
                return row.getMap(fieldName, String.class, Float.class);
            case DOUBLE:
                return row.getMap(fieldName, String.class, Double.class);
            case BOOLEAN:
                return row.getMap(fieldName, String.class, Boolean.class);
            case STRING:
                return row.getMap(fieldName, String.class, String.class);
            default:
                throw new UnsupportedOperationException(schema.getType() + " schema type not supported for avro Map Conversion");
        }
    }

    /**
     * This method is called when reading data from Cassandra. It should map a ResultSet into the
     * corresponding Java objects.
     *
     * @param resultSet A resultset containing rows.
     * @return An iterator containing the objects that you want to provide to your downstream
     * pipeline.
     */
    @SneakyThrows
    @Override
    public Iterator<GenericRecord> map(final ResultSet resultSet) {

        List<GenericRecord> genericRecords = new ArrayList<>();
        if (!resultSet.isExhausted()) {
            resultSet.iterator().forEachRemaining(r -> {
                counter(nodeName, nodeType, UNKNOWN, ELEMENTS_IN).inc();
                String schemaId = UNKNOWN;
                try {
                    GenericRecord record = fieldMapper(r, schema);
                    schemaId = record.getSchema().getFullName();
                    genericRecords.add(record);
                    incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, KEYSPACE + "_" + TABLE_NAME, aciCassandraOptions.getKeySpaceName() + "_" + aciCassandraOptions.getTableName(), SOURCE_SCHEMA_ID, schemaId);
                } catch (Exception e) {
                    LOGGER.error("Error converting ResultSet to genericRecord. Skip enabled!!", e);
                    if (aciCassandraOptions.isLogOnError()) LOGGER.trace("{}", r);
                    if (aciCassandraOptions.isThrowOnError()) {
                        counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                        throw new RuntimeException(e);
                    } else {
                        counter(nodeName, nodeType, schemaId, ELEMENTS_SKIPPED).inc();
                    }
                }
            });
        }

        return genericRecords.listIterator();
    }

    /**
     * This method is called for each delete event. The input argument is the Object that should be
     * deleted in Cassandra. The return value should be a Future that completes when the delete action
     * is completed.
     *
     * @param entity Entity to be deleted.
     */
    @Override
    public Future<Void> deleteAsync(final GenericRecord entity) {
        return null;
    }

    /**
     * This method is called for each save event. The input argument is the Object that should be
     * saved or updated in Cassandra. The return value should be a future that completes when the save
     * action is completed.
     *
     * @param entity Entity to be saved.
     */
    @Override
    public Future saveAsync(final GenericRecord entity) {
        String schemaId = entity.getSchema().getFullName();
        try {
            counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
            List<Schema.Field> fields = entity.getSchema().getFields();
            List<String> columns = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (Schema.Field field : fields) {
                columns.add(field.name());
                values.add(getFieldValue(entity, field.name()));
            }

            Insert insert = QueryBuilder.insertInto(aciCassandraOptions.getKeySpaceName(), aciCassandraOptions.getTableName()).values(columns, values);

            Statement statement = insert.setConsistencyLevel(ConsistencyLevel.valueOf(aciCassandraOptions.getConsistencyLevel())).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
            ResultSetFuture rsFuture = this.session.executeAsync(statement);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, KEYSPACE + "_" + TABLE_NAME, aciCassandraOptions.getKeySpaceName() + "_" + aciCassandraOptions.getTableName(), SOURCE_SCHEMA_ID, schemaId);
            return rsFuture;
        } catch (Exception e) {
            LOGGER.error("Error in writing to Cassandra!!", e);
            if (aciCassandraOptions.isLogOnError()) {
                LOGGER.trace("{}", entity);
            }
            if (aciCassandraOptions.isThrowOnError()) {
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                throw new RuntimeException(e);
            } else {
                counter(nodeName, nodeType, schemaId, ELEMENTS_SKIPPED).inc();
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private GenericRecord fieldMapper(final Row r, final Schema s) throws Exception {
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (Schema.Field f : s.getFields()) {
            genericRecord.put(f.name(), getRecord(f.schema().getType(), r, f.name(), f));
        }
        if (!GenericData.get().validate(genericRecord.getSchema(), genericRecord)) {
            throw new Exception("Generics Record validation failed");
        }
        return genericRecord;
    }

    private Object getRecord(final Schema.Type t, final Row row, String fieldName, final Schema.Field f) throws Exception {
        switch (t) {
            case STRING:
                return row.getString(fieldName);
            case INT:
                return row.getInt(fieldName);
            case LONG:
                return row.getLong(fieldName);
            case FLOAT:
                return row.getFloat(fieldName);
            case DOUBLE:
                return row.getDouble(fieldName);
            case BOOLEAN:
                return row.getBool(fieldName);
            case ARRAY:
                return fromAvroArray(f.schema().getElementType(), row, fieldName);
            case MAP:
                return fromAvroMap(f.schema().getValueType(), row, fieldName);
            case UNION:
                List<Schema> types = f.schema().getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return getRecord(type.getType(), row, fieldName, f);
                }
            case RECORD:
            case ENUM:
            default:
                throw new UnsupportedOperationException(t + " schema type not supported for avro conversion");
        }
    }
}
