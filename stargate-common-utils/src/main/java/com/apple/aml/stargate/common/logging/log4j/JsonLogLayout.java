package com.apple.aml.stargate.common.logging.log4j;

import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.utils.AppConfig;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.A3Constants.CALLER_APP_ID;
import static com.apple.aml.stargate.common.utils.AppConfig.appName;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static java.lang.Boolean.parseBoolean;

@Plugin(name = "JsonLogLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class JsonLogLayout extends AbstractStringLayout {
    private static String additionalLogString = env("APP_LOG_ADDITIONAL_PREFIX", null) == null ? "" : (" " + env("APP_LOG_ADDITIONAL_PREFIX", null));
    private final String globalAppId = AppConfig.appId() + "";
    private final String configRoot = configPrefix();
    private final String appNameString = ",\"appName\":\"" + env("APP_LOG_NAME", appName()).toLowerCase() + "\"";
    private final String appIdToken = config().hasPath(configRoot + ".logger.appId.token") ? config().getString(configRoot + ".logger.appId.token") : "appid";
    private final String stackTraceStdErrConfigName = configRoot + ".logger.stackTrace.print";
    private final boolean printStackTrace = parseBoolean(env(stackTraceStdErrConfigName, "false")) || (config().hasPath(stackTraceStdErrConfigName) && config().getBoolean(stackTraceStdErrConfigName));
    private final String stackTraceLogConfigName = configRoot + ".logger.stackTrace.log";
    private final boolean logStackTrace = parseBoolean(env(stackTraceLogConfigName, "true")) || (config().hasPath(stackTraceLogConfigName) && config().getBoolean(stackTraceLogConfigName));
    private final String threadNameConfigName = configRoot + ".logger.threadName.print";
    private final boolean printThreadName = parseBoolean(env(threadNameConfigName, "false")) || (config().hasPath(threadNameConfigName) && config().getBoolean(threadNameConfigName));
    private final String ignoreKeysConfigName = configRoot + ".logger.ignoreKeys";
    private final HashSet<String> ignoreKeys = AppConfig.config().hasPath(ignoreKeysConfigName) ? new HashSet<>(AppConfig.config().getStringList(ignoreKeysConfigName)) : new HashSet<>();
    private final boolean ignoreKeysEnabled = !ignoreKeys.isEmpty();
    private final char newLineReplacement = env("APP_LOG_NEWLINE_REPLACEMENT", "\r").charAt(0);

    public static void setAdditionalGlobalLogString(final String logString) {
        if (logString == null || logString.trim().isBlank()) {
            additionalLogString = "";
            return;
        }
        additionalLogString = logString.trim();
    }

    protected JsonLogLayout(final Charset charset) {
        super(charset);
    }

    @PluginFactory
    public static JsonLogLayout createLayout(@PluginAttribute(value = "charset", defaultString = "UTF-8") final Charset charset) {
        return new JsonLogLayout(charset);
    }

    @Override
    @SuppressWarnings("unchecked")
    public String toSerializable(final LogEvent event) {
        Object appId = null;
        List<Throwable> throwables = null;
        StringBuilder json = new StringBuilder("{");
        json.append("\"timestamp\":\"" + Instant.ofEpochMilli(event.getTimeMillis()) + "\"");
        json.append(",\"").append(appIdToken).append("\":\"").append((appId == null ? globalAppId : appId)).append("\"");
        json.append(appNameString);
        json.append(additionalLogString);
        json.append(",\"").append("level").append("\":\"").append(event.getLevel().name()).append("\"");
        json.append(",\"").append("logger").append("\":\"").append(event.getLoggerName()).append("\"");
        if (printThreadName) {
            json.append(",\"").append("thread").append("\":\"").append(event.getThreadName()).append("\"");
        }
        json.append(",\"").append("msg").append("\":\"").append(event.getMessage().getFormattedMessage().replace('"', '\'').replace('\n', ' ')).append("\"");
        if (event.getMessage().getParameters() != null && !(event.getMessage().getParameters().length == 1 && event.getMessage().getParameters()[0] instanceof String)) {
            int index = -1;
            for (Object arg : event.getMessage().getParameters()) {
                index++;
                if (arg == null) {
                    continue;
                }
                if (arg instanceof Map) {
                    Map<Object, Object> map = (Map) arg;
                    for (Map.Entry<Object, Object> entry : map.entrySet()) {
                        if (entry.getKey() == null || (ignoreKeysEnabled && ignoreKeys.contains(entry.getKey()))) {
                            continue;
                        }
                        String key = entry.getKey().toString();
                        json.append(",\"").append(key).append("\":\"").append(String.valueOf(entry.getValue()).replace('"', '\'').replace('\n', ' ')).append("\"");
                        if (appId == null) {
                            if (appIdToken.equals(key)) {
                                appId = entry.getValue();
                            } else if (CALLER_APP_ID.equalsIgnoreCase(key)) {
                                appId = entry.getValue();
                            }
                        }
                    }
                } else if (arg instanceof Throwable) {
                    if (throwables == null) {
                        throwables = new ArrayList<>();
                    }
                    Throwable throwable = (Throwable) arg;
                    throwables.add(throwable);
                } else {
                    json.append(",\"arg").append(index).append("\":\"").append(String.valueOf(arg).replace('"', '\'').replace('\n', ' ')).append("\"");
                }
            }
        }
        if (event.getContextData() != null) {
            for (Map.Entry<String, String> entry : event.getContextData().toMap().entrySet()) {
                String key = entry.getKey();
                if (key == null || (ignoreKeysEnabled && ignoreKeys.contains(key))) {
                    continue;
                }
                json.append(",\"").append(key).append("\":\"").append(String.valueOf(entry.getValue()).replace('"', '\'').replace('\n', ' ')).append("\"");
                if (appId == null) {
                    if (appIdToken.equals(key)) {
                        appId = entry.getValue();
                    } else if (CALLER_APP_ID.equalsIgnoreCase(key)) {
                        appId = entry.getValue();
                    }
                }
            }
        }
        boolean printCallerData = false;
        if (event.getThrown() != null) {
            if (throwables == null) throwables = new ArrayList<>();
            throwables.add(event.getThrown());
        }
        if (throwables != null) {
            for (Throwable throwable : throwables) {
                json.append(",\"").append("exception").append("\":\"").append(String.valueOf(throwable.getMessage()).replace('"', '\'')).append("\"");
                logGenericExceptionDetails(throwable, json);
                if (logStackTrace) {
                    json.append(",\"").append("cause").append("\":\"");
                    Throwable t = throwable;
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw, true);
                    while (t != null) {
                        t.printStackTrace(pw);
                        pw.print('\r');
                        t = t.getCause();
                    }
                    json.append(sw.toString().replace('"', '\''));
                    json.append("\"");
                }
                if (printStackTrace) {
                    Throwable t = throwable;
                    printCallerData = true;
                    while (t != null) {
                        t.printStackTrace(System.err);
                        System.err.println();
                        t = t.getCause();
                    }
                }
            }
        }
        if (printCallerData) {
            StackTraceElement callerData = event.getSource();
            if (callerData != null) {
                System.err.println("CallerData ->");
                System.err.println(callerData);
                System.err.println();
            }
        }
        json.append("}");
        String line = json.toString().replace('\n', newLineReplacement);
        return line + "\n";
    }

    @SuppressWarnings("unchecked")
    private static void logGenericExceptionDetails(final Throwable throwable, final StringBuilder builder) {
        GenericException exception = ExceptionUtils.throwableOfType(throwable, GenericException.class);
        if (exception == null) {
            return;
        }
        Map<String, Object> map = (Map<String, Object>) exception.getDebugInfo();
        if (map != null) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                builder.append(",\"").append(entry.getKey()).append("\":\"").append(String.valueOf(entry.getValue()).replace('"', '\'').replace('\n', ' ')).append("\"");
            }
        }
        if (exception.getDebugPojo() != null) {
            builder.append(",\"").append("debugInfo").append("\":\"").append(String.valueOf(exception.getDebugPojo()).replace('"', '\'').replace('\n', ' ')).append("\"");
        }
    }
}
