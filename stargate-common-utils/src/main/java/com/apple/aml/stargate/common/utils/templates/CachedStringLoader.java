package com.apple.aml.stargate.common.utils.templates;

import freemarker.cache.TemplateLoader;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class CachedStringLoader implements TemplateLoader {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long INIT_TIME = System.currentTimeMillis();
    private static final Cache<String, String> TEMPLATE_CACHE = new Cache2kBuilder<String, String>() {
    }.eternal(true).build();

    public void putTemplate(final String name, final String templateContent) {
        try {
            TEMPLATE_CACHE.put(name, templateContent);
        } catch (Exception e) {
            LOGGER.warn("Could not load template", Map.of("name", String.valueOf(name), "nullContent", templateContent == null, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
        }
    }

    @Override
    public Object findTemplateSource(final String name) throws IOException {
        return TEMPLATE_CACHE.get(name) == null ? null : name;
    }

    @Override
    public long getLastModified(final Object templateSource) {
        return INIT_TIME;
    }

    @Override
    public Reader getReader(final Object templateSource, final String encoding) throws IOException {
        String content = null;
        try {
            content = TEMPLATE_CACHE.get((String) templateSource);
            return new StringReader(content);
        } catch (Exception e) {
            LOGGER.warn("Could not read template", Map.of("name", String.valueOf(templateSource), "nullContent", content == null, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            return null;
        }
    }

    @Override
    public void closeTemplateSource(final Object templateSource) throws IOException {

    }
}
