package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.utils.templates.CachedStringLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class FreemarkerUtils {
    private static final CachedStringLoader TEMPLATE_LOADER = new CachedStringLoader();
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final TemplateExceptionHandler EXCEPTION_HANDLER = (exception, env, out) -> {
        if (LOGGER.isTraceEnabled()) LOGGER.trace("error occurred during freemarker evaluation", Map.of(ERROR_MESSAGE, String.valueOf(exception.getMessage())), exception);
        throw exception;
    };
    private static final Configuration CONFIGURATION = getFreeMarkerConfiguration(TEMPLATE_LOADER, EXCEPTION_HANDLER);

    private FreemarkerUtils() {
    }

    @SuppressWarnings("unchecked")
    public static Configuration freeMarkerConfiguration() {
        return CONFIGURATION;
    }

    public static void loadFreemarkerTemplate(final String templateName, final String templateContent) {
        TEMPLATE_LOADER.putTemplate(templateName, templateContent);
    }

    @SuppressWarnings("unchecked")
    public static Configuration getFreeMarkerConfiguration(final TemplateLoader templateLoader, final TemplateExceptionHandler exceptionHandler) {
        Configuration configuration = new Configuration(Configuration.VERSION_2_3_32);
        configuration.setDefaultEncoding(StandardCharsets.UTF_8.name());
        configuration.setTemplateExceptionHandler(exceptionHandler);
        configuration.setLogTemplateExceptions(false);
        configuration.setWrapUncheckedExceptions(true);
        configuration.setFallbackOnNullLoopVariable(false);
        configuration.setNumberFormat("computer");
        configuration.setBooleanFormat("c");
        configuration.setTemplateLoader(templateLoader);
        return configuration;
    }
}
