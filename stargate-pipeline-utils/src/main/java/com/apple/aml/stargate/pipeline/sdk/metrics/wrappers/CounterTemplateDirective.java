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

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.COUNTER;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;

public class CounterTemplateDirective implements BaseTemplateDirectiveModel {
    private final CounterWrapper wrapper;

    public CounterTemplateDirective() {
        this(null);
    }

    public CounterTemplateDirective(final CounterWrapper wrapper) {
        this.wrapper = wrapper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(final Environment env, final Map params, final TemplateModel[] loopVars, final TemplateDirectiveBody body) throws TemplateException, IOException {
        CounterWrapper counter = this.wrapper == null ? ((CounterTemplateDirective) env.__getitem__(COUNTER)).wrapper() : this.wrapper;
        applyCounter(env, (Map<String, TemplateModel>) params, loopVars, body, counter);
    }

    public CounterWrapper wrapper() {
        return wrapper;
    }

    static void applyCounter(final Environment env, final Map<String, TemplateModel> params, final TemplateModel[] loopVars, final TemplateDirectiveBody body, final CounterWrapper counter) throws TemplateException, IOException {
        for (TemplateModel model : loopVars) {
            if (model instanceof TemplateScalarModel) {
                counter.inc(((TemplateScalarModel) model).getAsString());
            } else if (model instanceof TemplateNumberModel) {
                counter.inc(((TemplateNumberModel) model).getAsNumber().toString());
            }
        }
        for (Map.Entry<String, TemplateModel> entry : params.entrySet()) {
            if (entry.getValue() instanceof TemplateNumberModel) {
                counter.incBy(entry.getKey(), ((TemplateNumberModel) entry.getValue()).getAsNumber().intValue());
            }
        }
        if (body != null) {
            CounterWriter writer = new CounterWriter(env.getOut(), counter);
            body.render(writer);
            StringBuffer buffer = writer.buffer();
            if (buffer != null) {
                applyOnContent(buffer, counter);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void applyOnContent(final StringBuffer buffer, final CounterWrapper counter) throws JsonProcessingException {
        String jsonString = buffer.toString().trim();
        if (jsonString.startsWith("{")) {
            for (Map.Entry<Object, Object> entry : readJsonMap(jsonString).entrySet()) {
                counter.incBy(entry.getKey().toString().trim(), entry.getValue() instanceof Integer ? (Integer) entry.getValue() : Integer.valueOf(entry.getValue().toString().trim()));
            }
            return;
        }
        for (String key : (List<String>) readJson(jsonString, List.class)) {
            counter.inc(key.trim());
        }
    }

    @Override
    public String type() {
        return COUNTER;
    }

    private static class CounterWriter extends Writer {
        private final Writer out;
        private final CounterWrapper counter;
        private StringBuffer buffer;

        CounterWriter(final Writer out, final CounterWrapper counter) {
            this.out = out;
            this.counter = counter;
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
            applyOnContent(buffer, counter);
            buffer = null;
        }

        StringBuffer buffer() {
            return buffer;
        }
    }
}
