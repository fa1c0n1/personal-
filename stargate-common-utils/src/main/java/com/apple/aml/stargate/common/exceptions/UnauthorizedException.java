package com.apple.aml.stargate.common.exceptions;

import java.util.Map;

public class UnauthorizedException extends GenericException {

    public UnauthorizedException(final String message) {
        super(message);
    }

    public UnauthorizedException(final String message, final Map<String, ?> debugInfo) {
        super(message, debugInfo);
    }

    public UnauthorizedException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        super(message, debugInfo, cause);
    }

    public UnauthorizedException(final String message, final Object debugPojo) {
        super(message, debugPojo);
    }

    public UnauthorizedException(final String message, final Object debugPojo, final Throwable cause) {
        super(message, debugPojo, cause);
    }
}
