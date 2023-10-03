package com.apple.aml.stargate.common.pojo;

import java.util.Map;

public class ResponseBody<O> {
    private final boolean error;
    private final Map<String, O> debugInfo;
    private String statusMessage;

    public ResponseBody(final String statusMessage) {
        this(statusMessage, null);
    }

    public ResponseBody(final String statusMessage, final Map<String, O> debugInfo) {
        this(statusMessage, debugInfo, false);
    }

    public ResponseBody(final String statusMessage, final Map<String, O> debugInfo, final boolean error) {
        this.statusMessage = statusMessage;
        this.debugInfo = debugInfo;
        this.error = error;
    }

    public ResponseBody(final String statusMessage, final boolean error) {
        this(statusMessage, null, error);
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(final String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public Map<String, O> getDebugInfo() {
        return debugInfo;
    }

    public boolean error() {
        return this.error;
    }
}
