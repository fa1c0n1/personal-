package com.apple.aml.stargate.common.exceptions;

import java.util.Map;

public class InvalidInputException extends GenericException {
    public InvalidInputException(final String message) {
        super(message);
    }

    public InvalidInputException(final String message, final Map<String, ?> debugInfo) {
        super(message, debugInfo);
    }

    public InvalidInputException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        super(message, debugInfo, cause);
    }

    public InvalidInputException(final String message, final Object debugPojo) {
        super(message, debugPojo);
    }

    public InvalidInputException(final String message, final Object debugPojo, final Throwable cause) {
        super(message, debugPojo, cause);
    }
}
