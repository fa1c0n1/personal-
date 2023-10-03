package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.SolrOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.REDACTED_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;


public class InbuiltSolrTransformer extends DoFn<SolrDocument, KV<String, GenericRecord>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final SolrOptions options;
    private final ObjectToGenericRecordConverter converter;
    private final String schemaId;
    private final Schema schema;
    private final String nodeName;
    private final String nodeType;
    private final String keyTemplateName;
    private Configuration configuration;

    public InbuiltSolrTransformer(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final SolrOptions options) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.options = duplicate(options, SolrOptions.class);
        this.options.setPassword(REDACTED_STRING);
        if (this.options.getSchemaId() == null) {
            this.schema = null; // TODO
            this.schemaId = this.schema.getFullName();
        } else {
            this.schemaId = this.options.getSchemaId();
            this.schema = fetchSchemaWithLocalFallback(this.options.getSchemaReference(), schemaId);
        }
        this.converter = converter(this.schema);
        this.keyTemplateName = options.getKeyExpression() == null ? null : (nodeName + "~" + schemaId + "~~" + options.getCollection());
        LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final SolrDocument doc, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        counter(nodeName, nodeType, UNKNOWN, ELEMENTS_IN).inc();
        try {
            Configuration configuration = configuration();
            GenericRecord response = converter.convert(doc);
            String responseKey = keyTemplateName == null ? UUID.randomUUID().toString() : evaluateFreemarker(configuration, keyTemplateName, null, doc, schema);
            ctx.output(KV.of(responseKey, response));
            histogramDuration(nodeName, nodeType, response.getSchema().getFullName(), "process").observe((System.nanoTime() - startTime) / 1000000.0);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, UNKNOWN, "error").observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Could not convert/emit result solr doc to kv of String/GenericRecord", doc, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
        }
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (this.keyTemplateName != null) {
            loadFreemarkerTemplate(keyTemplateName, this.options.getKeyExpression());
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }
}
