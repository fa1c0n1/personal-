package com.apple.aml.stargate.beam.sdk.io.snowflake;

import com.apple.aml.stargate.beam.sdk.io.jdbc.GenericJdbcIO;
import com.apple.aml.stargate.beam.sdk.io.jdbc.JdbcRecordExecutor;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.SnowflakeOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class SnowflakeIO extends GenericJdbcIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        SnowflakeOptions options = (SnowflakeOptions) node.getConfig();
        options.enableSchemaLevelDefaults();
        return collection.apply(node.getName(), new JdbcRecordExecutor(node.getName(), node.getType(), options, true));
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        SnowflakeOptions options = (SnowflakeOptions) node.getConfig();
        options.enableSchemaLevelDefaults();
        return collection.apply(node.getName(), new JdbcRecordExecutor(node.getName(), node.getType(), options, false));
    }
}
