package com.apple.aml.stargate.common.exceptions;

import java.util.Map;

public class GenericException extends Exception {
    private final Map<String, ?> debugInfo;
    private final Object debugPojo;

    public GenericException(final String message) {
        super(message);
        this.debugInfo = null;
        this.debugPojo = null;
    }

    public GenericException(final String message, final Map<String, ?> debugInfo) {
        super(message);
        this.debugInfo = debugInfo;
        this.debugPojo = null;
    }

    public GenericException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        super(message, cause);
        this.debugInfo = debugInfo;
        this.debugPojo = null;
    }

    public GenericException(final String message, final Object debugPojo) {
        super(message);
        this.debugInfo = null;
        this.debugPojo = debugPojo;
    }

    public GenericException(final String message, final Object debugPojo, final Throwable cause) {
        super(message, cause);
        this.debugInfo = null;
        this.debugPojo = debugPojo;
    }

    public Map<String, ?> getDebugInfo() {
        return debugInfo;
    }

    public Object getDebugPojo() {
        return debugPojo;
    }

    public GenericRuntimeException wrap() {
        return new GenericRuntimeException(this);
    }

}
