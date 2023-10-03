package com.apple.aml.stargate.common.exceptions;

import java.util.Map;

public class ResourceNotFoundException extends GenericException {

    public ResourceNotFoundException(final String message) {
        super(message);
    }

    public ResourceNotFoundException(final String message, final Map<String, ?> debugInfo) {
        super(message, debugInfo);
    }

    public ResourceNotFoundException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        super(message, debugInfo, cause);
    }

    public ResourceNotFoundException(final String message, final Object debugPojo) {
        super(message, debugPojo);
    }

    public ResourceNotFoundException(final String message, final Object debugPojo, final Throwable cause) {
        super(message, debugPojo, cause);
    }
}
