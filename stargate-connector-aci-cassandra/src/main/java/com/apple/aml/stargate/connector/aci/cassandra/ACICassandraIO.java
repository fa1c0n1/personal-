package com.apple.aml.stargate.connector.aci.cassandra;

import com.apple.aml.stargate.beam.sdk.coders.AvroCoder;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.CommonConstants;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.ACICassandraOptions;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.datastax.driver.core.Session;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;


public class ACICassandraIO implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    public Map<String, Object> initRead(final Pipeline pipeline, final StargateNode node) throws Exception {
        return new HashMap<>();
    }

    public Map<String, Object> initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        return new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {

        final ACICassandraOptions options = (ACICassandraOptions) node.getConfig();

        Schema schema = fetchSchemaWithLocalFallback(null, options.getSchemaId());
        SerializableFunction<Session, Mapper> factory = new GenericMapperFactory(node.getName(), node.getType(), schema, options);

        if (isBlank(options.getRecordIdentifier()) && CollectionUtils.isEmpty(options.getPrimaryKeys())) {
            throw new GenericException("Record Identifier null or empty");
        }

        org.apache.beam.sdk.io.cassandra.CassandraIO.Read<GenericRecord> read = org.apache.beam.sdk.io.cassandra.CassandraIO.read();

        read = read.withVanillaCassandra(options.isVanilla());
        if (null != options.getPort()) {
            read = read.withPort(options.getPort());
        }

        if (!isBlank(options.getKeySpaceName())) {
            read = read.withKeyspace(options.getKeySpaceName());
        }

        if (!isBlank(options.getTableName())) {
            read = read.withTable(options.getTableName());
        }

        if (!isBlank(options.getUserName())) {
            read = read.withUsername(options.getUserName());
        }

        if (!isBlank(options.getPassword())) {
            read = read.withPassword(options.getPassword());
        }

        if (!isBlank(options.getDc())) {
            read = read.withLocalDc(options.getDc());
        }

        read = read.withMapperFactoryFn(factory);

        if (options.isVanilla()) {
            if (null != options.getHosts()) {
                read = read.withHosts(options.getHosts());
            }

        } else {

            read = read.withHosts(getHosts(options));

            if (!isBlank(options.getApplicationName())) {
                read = read.withApplicationName(options.getApplicationName());
            }

            if (!isBlank(options.getClusterName())) {
                read = read.withClusterName(options.getClusterName());
            }

            if (!isBlank(options.getTruststoreLocation())) {
                read = read.withTrustStoreLocation(options.getTruststoreLocation());
            }
            if (!isBlank(options.getTruststorePassword())) {
                read = read.withTrustStorePassword(options.getTruststorePassword());
            }
            if (!isBlank(options.getCasseroleURL())) {
                read = read.withDiscoveryUrl(options.getCasseroleURL());
            }
        }

        if (options.getConnectTimeout() > 0) {
            read = read.withConnectTimeout(options.getConnectTimeout());
        }
        if (options.getReadTimeout() > 0) {
            read = read.withReadTimeout(options.getReadTimeout());
        }
        if (options.getMinNumberOfSplits() > 0) {
            read = read.withMinNumberOfSplits(options.getMinNumberOfSplits());
        }
        if (!isBlank(options.getConsistencyLevel())) {
            read = read.withConsistencyLevel(options.getConsistencyLevel());
        }
        if (!isBlank(options.getQuery())) {
            read = read.withQuery(options.getQuery());
        }

        read = read.withCoder(AvroCoder.of(options.getSchemaReference()));
        read = read.withEntity(GenericRecord.class);

        SCollection<GenericRecord> contentCollection = SCollection.apply(pipeline, node.name("reader"), read);

        String nodeName = node.getName();
        String nodeType = node.getType();

        return contentCollection.apply(nodeName, new DoFn<GenericRecord, KV<String, GenericRecord>>() {

            @ProcessElement
            public void processElement(@Element final GenericRecord genericRecord, final ProcessContext ctx) throws Exception {
                String schemaId = genericRecord.getSchema().getFullName();
                counter(nodeName, nodeType, genericRecord.getSchema().getFullName(), ELEMENTS_IN).inc();
                ctx.output(KV.of(getRecordIdentifier(genericRecord, options), genericRecord));
                incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, CommonConstants.MetricLabels.SOURCE_SCHEMA_ID, schemaId);
            }

            private String getRecordIdentifier(GenericRecord genericRecord, ACICassandraOptions options) {
                if (!CollectionUtils.isEmpty(options.getPrimaryKeys())) {
                    StringBuilder primaryKeyBuilder = new StringBuilder();
                    for (String keys : options.getPrimaryKeys()) {
                        primaryKeyBuilder.append(genericRecord.get(keys)).append("_");
                    }
                    return primaryKeyBuilder.toString();
                } else {
                    return genericRecord.get(options.getRecordIdentifier()).toString();
                }
            }
        });
    }

    private List<String> getHosts(final ACICassandraOptions options) throws Exception {

        String url = options.getCasseroleURL() + CassandraConstants.DiscoveryService.BASE_PATH + options.getApplicationName() + "/" + options.getClusterName() + "/" + CassandraConstants.DiscoveryService.CONTEXT_PATH + "?" + CassandraConstants.DiscoveryService.REQUEST_PARAM;

        String hosts = WebUtils.httpGet(url, null, String.class, true, true);
        JsonElement element = JsonParser.parseString(hosts);

        Type listType = new TypeToken<List<String>>() {
        }.getType();

        List<String> hostList = new Gson().fromJson(element, listType);
        return hostList;

    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {

        final ACICassandraOptions options = (ACICassandraOptions) node.getConfig();

        Schema schema = fetchSchemaWithLocalFallback(null, options.getSchemaId());
        SerializableFunction<Session, Mapper> factory = new GenericMapperFactory(node.getName(), node.getType(), schema, options);

        org.apache.beam.sdk.io.cassandra.CassandraIO.Write<GenericRecord> write = org.apache.beam.sdk.io.cassandra.CassandraIO.write();

        write = write.withVanillaCassandra(options.isVanilla());
        if (null != options.getPort()) {
            write = write.withPort(options.getPort());
        }

        if (!isBlank(options.getKeySpaceName())) {
            write = write.withKeyspace(options.getKeySpaceName());
        }

        if (!isBlank(options.getTableName())) {
            write = write.withTable(options.getTableName());
        }

        if (!isBlank(options.getUserName())) {
            write = write.withUsername(options.getUserName());
        }

        if (!isBlank(options.getPassword())) {
            write = write.withPassword(options.getPassword());
        }

        if (!isBlank(options.getDc())) {
            write = write.withLocalDc(options.getDc());
        }

        write = write.withMapperFactoryFn(factory);

        if (options.isVanilla()) {
            if (null != options.getHosts()) {
                write = write.withHosts(options.getHosts());
            }

        } else {

            write = write.withHosts(getHosts(options));

            if (!isBlank(options.getApplicationName())) {
                write = write.withApplicationName(options.getApplicationName());
            }

            if (!isBlank(options.getClusterName())) {
                write = write.withClusterName(options.getClusterName());
            }

            if (!isBlank(options.getTruststoreLocation())) {
                write = write.withTrustStoreLocation(options.getTruststoreLocation());
            }
            if (!isBlank(options.getTruststorePassword())) {
                write = write.withTrustStorePassword(options.getTruststorePassword());
            }
            if (!isBlank(options.getCasseroleURL())) {
                write = write.withDiscoveryUrl(options.getCasseroleURL());
            }
        }

        if (options.getConnectTimeout() > 0) {
            write = write.withConnectTimeout(options.getConnectTimeout());
        }
        if (options.getReadTimeout() > 0) {
            write = write.withReadTimeout(options.getReadTimeout());
        }
        if (!isBlank(options.getConsistencyLevel())) {
            write = write.withConsistencyLevel(options.getConsistencyLevel());
        }

        write = write.withEntity(GenericRecord.class);
        final String nodeName = node.getName();
        final String nodeType = node.getType();
        return collection.apply(node.name("extract-record"), new DoFn<KV<String, GenericRecord>, GenericRecord>() {
            @ProcessElement
            public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
                log(options, nodeName, nodeType, kv);
                ctx.output(kv.getValue());
            }
        }, GENERIC_RECORD_TAG).done(node.getName(), write);
    }
}
