package com.apple.aml.stargate.common.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.LayoutBase;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.utils.AppConfig;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
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

public final class JsonLogLayout extends LayoutBase<ILoggingEvent> {
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public String doLayout(final ILoggingEvent event) {
        Object appId = null;
        List<Throwable> throwables = null;
        StringBuilder json = new StringBuilder("{");
        json.append("\"timestamp\":\"" + Instant.ofEpochMilli(event.getTimeStamp()) + "\"");
        json.append(",\"").append(appIdToken).append("\":\"").append((appId == null ? globalAppId : appId)).append("\"");
        json.append(appNameString);
        json.append(additionalLogString);
        json.append(",\"").append("level").append("\":\"").append(event.getLevel().levelStr).append("\"");
        json.append(",\"").append("logger").append("\":\"").append(event.getLoggerName()).append("\"");
        if (printThreadName) {
            json.append(",\"").append("thread").append("\":\"").append(event.getThreadName()).append("\"");
        }
        json.append(",\"").append("msg").append("\":\"").append(event.getFormattedMessage().replace('"', '\'').replace('\n', ' ')).append("\"");
        if (event.getArgumentArray() != null && !(event.getArgumentArray().length == 1 && event.getArgumentArray()[0] instanceof String)) {
            int index = -1;
            for (Object arg : event.getArgumentArray()) {
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
        if (event.getMDCPropertyMap() != null) {
            for (Map.Entry<String, String> entry : event.getMDCPropertyMap().entrySet()) {
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
        if (event.getThrowableProxy() != null) {
            IThrowableProxy proxy = event.getThrowableProxy();
            Throwable throwable = null;
            if (proxy instanceof ThrowableProxy) {
                ThrowableProxy throwableProxy = (ThrowableProxy) proxy;
                if (throwableProxy.getThrowable() != null) {
                    throwable = throwableProxy.getThrowable();
                    if (throwables == null) throwables = new ArrayList<>();
                    throwables.add(throwable);
                }
            }
            if (throwable == null) {
                json.append(",\"").append("exception").append("\":\"").append(String.valueOf(proxy.getMessage()).replace('"', '\'').replace('\n', ' ')).append("\"");
                if (logStackTrace) {
                    json.append(",\"").append("cause").append("\":\"");
                    IThrowableProxy t = proxy;
                    StringBuffer traceBuffer = new StringBuffer();
                    while (t != null && t.getStackTraceElementProxyArray() != null) {
                        for (StackTraceElementProxy stack : t.getStackTraceElementProxyArray()) {
                            StackTraceElement ste = stack.getStackTraceElement();
                            traceBuffer.append(ste.toString().replace('"', '\'')).append("\n");
                        }
                        traceBuffer.append("\n");
                        t = t.getCause();
                    }
                    json.append(traceBuffer);
                    json.append("\"");
                }
                if (printStackTrace) {
                    IThrowableProxy t = proxy;
                    while (t != null && t.getStackTraceElementProxyArray() != null) {
                        for (StackTraceElementProxy stack : t.getStackTraceElementProxyArray()) {
                            StackTraceElement ste = stack.getStackTraceElement();
                            System.err.println(ste);
                        }
                        System.err.println();
                        t = t.getCause();
                    }
                    printCallerData = true;
                }
            }
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
            StackTraceElement[] callerData = event.getCallerData();
            if (callerData != null && callerData.length > 0) {
                System.err.println("CallerData ->");
                for (StackTraceElement ste : callerData) {
                    System.err.println(ste);
                }
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
