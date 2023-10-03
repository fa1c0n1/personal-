package com.apple.aml.stargate.common.exceptions;

import java.util.Map;

public class BadRequestException extends GenericException {

    public BadRequestException(final String message) {
        super(message);
    }

    public BadRequestException(final String message, final Map<String, ?> debugInfo) {
        super(message, debugInfo);
    }

    public BadRequestException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        super(message, debugInfo, cause);
    }

    public BadRequestException(final String message, final Object debugPojo) {
        super(message, debugPojo);
    }

    public BadRequestException(final String message, final Object debugPojo, final Throwable cause) {
        super(message, debugPojo, cause);
    }
}
