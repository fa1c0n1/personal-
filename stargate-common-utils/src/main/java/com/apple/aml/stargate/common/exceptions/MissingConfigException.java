package com.apple.aml.stargate.common.exceptions;

import java.util.Map;

public class MissingConfigException extends GenericException {
    private final int statusCode;

    public MissingConfigException(final String message) {
        super(message);
        this.statusCode = 0;
    }

    public MissingConfigException(final String message, final Map<String, ?> debugInfo) {
        this(message, debugInfo, null, 0);
    }

    public MissingConfigException(final String message, final Map<String, ?> debugInfo, final Throwable cause, final int statusCode) {
        super(message, debugInfo, cause);
        this.statusCode = statusCode;
    }

    public MissingConfigException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        this(message, debugInfo, cause, 0);
    }

    public MissingConfigException(final String message, final Object debugPojo) {
        this(message, debugPojo, null, 0);
    }

    public MissingConfigException(final String message, final Object debugPojo, final Throwable cause, final int statusCode) {
        super(message, debugPojo, cause);
        this.statusCode = statusCode;
    }

    public MissingConfigException(final String message, final Object debugPojo, final Throwable cause) {
        this(message, debugPojo, cause, 0);
    }

    public int getStatusCode() {
        return statusCode;
    }
}
