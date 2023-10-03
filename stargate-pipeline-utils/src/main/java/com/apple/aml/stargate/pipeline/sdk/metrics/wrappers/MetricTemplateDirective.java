package com.apple.aml.stargate.pipeline.sdk.metrics.wrappers;

import freemarker.core.Environment;
import freemarker.template.TemplateDirectiveBody;
import freemarker.template.TemplateDirectiveModel;
import freemarker.template.TemplateException;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateScalarModel;

import java.io.IOException;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.COUNTER;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.GAUGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.HISTOGRAM;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.TYPE;
import static com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.CounterTemplateDirective.applyCounter;
import static com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.GaugeTemplateDirective.applyGauge;
import static com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.HistogramTemplateDirective.applyHistogram;

public class MetricTemplateDirective implements TemplateDirectiveModel {
    @SuppressWarnings("unchecked")
    @Override
    public void execute(final Environment env, final Map iParams, final TemplateModel[] loopVars, final TemplateDirectiveBody body) throws TemplateException, IOException {
        Object typeObject = iParams.get(TYPE);
        if (typeObject == null) return;
        String type = typeObject instanceof BaseTemplateDirectiveModel ? ((BaseTemplateDirectiveModel) typeObject).type() : typeObject instanceof TemplateScalarModel ? ((TemplateScalarModel) typeObject).getAsString().trim() : null;
        Map<String, TemplateModel> params = (Map<String, TemplateModel>) iParams;
        if (COUNTER.equalsIgnoreCase(type)) {
            applyCounter(env, params, loopVars, body, ((CounterTemplateDirective) env.__getitem__(COUNTER)).wrapper());
            return;
        }
        if (HISTOGRAM.equalsIgnoreCase(type)) {
            applyHistogram(env, params, loopVars, body, ((HistogramTemplateDirective) env.__getitem__(HISTOGRAM)).wrapper());
            return;
        }
        if (GAUGE.equalsIgnoreCase(type)) {
            applyGauge(env, params, loopVars, body, ((GaugeTemplateDirective) env.__getitem__(GAUGE)).wrapper());
            return;
        }
        throw new IOException(String.format("Unknown metric Type : %s", type));
    }
}
