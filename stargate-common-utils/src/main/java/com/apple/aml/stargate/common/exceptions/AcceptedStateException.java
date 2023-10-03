package com.apple.aml.stargate.common.exceptions;

import java.util.Map;

public class AcceptedStateException extends GenericException {

    public AcceptedStateException(final String message) {
        super(message);
    }

    public AcceptedStateException(final String message, final Map<String, ?> debugInfo) {
        super(message, debugInfo);
    }

    public AcceptedStateException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        super(message, debugInfo, cause);
    }

    public AcceptedStateException(final String message, final Object debugPojo) {
        super(message, debugPojo);
    }

    public AcceptedStateException(final String message, final Object debugPojo, final Throwable cause) {
        super(message, debugPojo, cause);
    }

}
