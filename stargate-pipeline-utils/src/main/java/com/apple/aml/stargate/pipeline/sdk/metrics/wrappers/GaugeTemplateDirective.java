package com.apple.aml.stargate.pipeline.sdk.metrics.wrappers;

import com.fasterxml.jackson.core.JsonProcessingException;
import freemarker.core.Environment;
import freemarker.template.TemplateDirectiveBody;
import freemarker.template.TemplateException;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateNumberModel;
import freemarker.template.TemplateScalarModel;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.GAUGE;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;

public class GaugeTemplateDirective implements BaseTemplateDirectiveModel {

    private final GaugeWrapper wrapper;

    public GaugeTemplateDirective() {
        this(null);
    }

    public GaugeTemplateDirective(final GaugeWrapper wrapper) {
        this.wrapper = wrapper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(final Environment env, final Map params, final TemplateModel[] loopVars, final TemplateDirectiveBody body) throws TemplateException, IOException {
        GaugeWrapper gauge = this.wrapper == null ? ((GaugeTemplateDirective) env.__getitem__(GAUGE)).wrapper() : this.wrapper;
        applyGauge(env, (Map<String, TemplateModel>) params, loopVars, body, gauge);
    }

    public GaugeWrapper wrapper() {
        return wrapper;
    }

    static void applyGauge(final Environment env, final Map<String, TemplateModel> params, final TemplateModel[] loopVars, final TemplateDirectiveBody body, final GaugeWrapper gauge) throws TemplateException, IOException {
        for (TemplateModel model : loopVars) {
            if (model instanceof TemplateScalarModel) {
                gauge.inc(((TemplateScalarModel) model).getAsString());
            } else if (model instanceof TemplateNumberModel) {
                gauge.inc(((TemplateNumberModel) model).getAsNumber().toString());
            }
        }
        for (Map.Entry<String, TemplateModel> entry : params.entrySet()) {
            if (entry.getValue() instanceof TemplateNumberModel) {
                gauge.set(entry.getKey(), ((TemplateNumberModel) entry.getValue()).getAsNumber().intValue());
            }
        }
        if (body != null) {
            GaugeWriter writer = new GaugeWriter(env.getOut(), gauge);
            body.render(writer);
            StringBuffer buffer = writer.buffer();
            if (buffer != null) {
                applyOnContent(buffer, gauge);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void applyOnContent(final StringBuffer buffer, final GaugeWrapper gauge) throws JsonProcessingException {
        String jsonString = buffer.toString().trim();
        if (jsonString.startsWith("{")) {
            for (Map.Entry<Object, Object> entry : readJsonMap(jsonString).entrySet()) {
                gauge.set(entry.getKey().toString().trim(), entry.getValue() instanceof Double ? (Double) entry.getValue() : Double.valueOf(entry.getValue().toString().trim()));
            }
            return;
        }
        for (String key : (List<String>) readJson(jsonString, List.class)) {
            gauge.inc(key.trim());
        }
    }

    @Override
    public String type() {
        return GAUGE;
    }

    private static class GaugeWriter extends Writer {
        private final Writer out;
        private final GaugeWrapper gauge;
        private StringBuffer buffer;

        GaugeWriter(final Writer out, final GaugeWrapper gauge) {
            this.out = out;
            this.gauge = gauge;
            this.buffer = new StringBuffer();
        }

        @Override
        public void write(final char[] cbuf, final int off, final int len) throws IOException {
            buffer.append(cbuf, off, len);
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void close() throws IOException {
            out.close();
            applyOnContent(buffer, gauge);
            buffer = null;
        }

        StringBuffer buffer() {
            return buffer;
        }
    }
}
