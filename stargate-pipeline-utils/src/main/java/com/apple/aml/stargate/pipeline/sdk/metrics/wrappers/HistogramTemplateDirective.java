package com.apple.aml.stargate.pipeline.sdk.metrics.wrappers;

import com.fasterxml.jackson.core.JsonProcessingException;
import freemarker.core.Environment;
import freemarker.template.TemplateDirectiveBody;
import freemarker.template.TemplateException;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateNumberModel;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.HISTOGRAM;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;

public class HistogramTemplateDirective implements BaseTemplateDirectiveModel {

    private final HistogramWrapper wrapper;

    public HistogramTemplateDirective() {
        this(null);
    }

    public HistogramTemplateDirective(final HistogramWrapper wrapper) {
        this.wrapper = wrapper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(final Environment env, final Map params, final TemplateModel[] loopVars, final TemplateDirectiveBody body) throws TemplateException, IOException {
        HistogramWrapper histogram = this.wrapper == null ? ((HistogramTemplateDirective) env.__getitem__(HISTOGRAM)).wrapper() : this.wrapper;
        applyHistogram(env, (Map<String, TemplateModel>) params, loopVars, body, histogram);
    }

    public HistogramWrapper wrapper() {
        return wrapper;
    }

    static void applyHistogram(final Environment env, final Map<String, TemplateModel> params, final TemplateModel[] loopVars, final TemplateDirectiveBody body, final HistogramWrapper histogram) throws TemplateException, IOException {
        for (Map.Entry<String, TemplateModel> entry : params.entrySet()) {
            if (entry.getValue() instanceof TemplateNumberModel) {
                histogram.observe(entry.getKey(), ((TemplateNumberModel) entry.getValue()).getAsNumber().intValue());
            }
        }
        if (body != null) {
            HistogramWriter writer = new HistogramWriter(env.getOut(), histogram);
            body.render(writer);
            StringBuffer buffer = writer.buffer();
            if (buffer != null) {
                applyOnContent(buffer, histogram);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void applyOnContent(final StringBuffer buffer, final HistogramWrapper histogram) throws JsonProcessingException {
        String jsonString = buffer.toString().trim();
        for (Map.Entry<Object, Object> entry : readJsonMap(jsonString).entrySet()) {
            histogram.observe(entry.getKey().toString().trim(), entry.getValue() instanceof Double ? (Double) entry.getValue() : Double.valueOf(entry.getValue().toString().trim()));
        }
    }

    @Override
    public String type() {
        return HISTOGRAM;
    }

    private static class HistogramWriter extends Writer {
        private final Writer out;
        private final HistogramWrapper histogram;
        private StringBuffer buffer;

        HistogramWriter(final Writer out, final HistogramWrapper histogram) {
            this.out = out;
            this.histogram = histogram;
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
            applyOnContent(buffer, histogram);
            buffer = null;
        }

        StringBuffer buffer() {
            return buffer;
        }
    }
}