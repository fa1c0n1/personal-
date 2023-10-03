package com.apple.aml.stargate.connector.aci.cassandra;

import com.apple.aml.stargate.common.options.ACICassandraOptions;
import com.datastax.driver.core.Session;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.transforms.SerializableFunction;

@SuppressWarnings({"rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
        "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class GenericMapperFactory implements SerializableFunction<Session, Mapper> {

    private final Schema schema;
    private final String nodeName;
    private final String nodeType;
    private final ACICassandraOptions options;

    public GenericMapperFactory(final String nodeName, final String nodeType, final Schema schema, final ACICassandraOptions aciCassandraOptions) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.schema = schema;
        this.options = aciCassandraOptions;
    }

    @Override
    public Mapper apply(final Session session) {
        return new GenericRecordObjectMapper(nodeName, nodeType, schema, session, options);
    }
}
