package com.apple.aml.stargate.beam.sdk.predicates;

import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;

public class FreemarkerPredicate implements SerializableFunction<KV<String, GenericRecord>, Boolean> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private final String templateName;
    private final String expression;
    private transient Configuration configuration;

    public FreemarkerPredicate(final String expression, final String nodeName) {
        this.nodeName = nodeName;
        this.expression = expression;
        this.templateName = nodeName + "~predicate";
    }

    @SuppressWarnings("deprecation")
    @Override
    public Boolean apply(KV<String, GenericRecord> kv) {
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            String outputString = evaluateFreemarker(configuration(), templateName, kv.getKey(), record, schema);
            return parseBoolean(outputString.trim());
        } catch (Exception e) {
            LOGGER.debug("Exception evaluating freemarker predicate. Will assume false and filter this record", Map.of("key", kv.getKey(), NODE_NAME, nodeName, ERROR_MESSAGE, String.valueOf(e.getMessage())));
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        loadFreemarkerTemplate(templateName, this.expression);
        configuration = freeMarkerConfiguration();
        return configuration;
    }
}
