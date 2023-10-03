package com.apple.aml.stargate.cache.exception;

import com.apple.aml.stargate.common.exceptions.GenericException;

import java.util.Map;

public class CacheWriteException extends GenericException {

    public CacheWriteException(final String message) {
        super(message);
    }

    public CacheWriteException(final String message, final Map<String, ?> debugInfo) {
        super(message, debugInfo);
    }

    public CacheWriteException(final String message, final Map<String, ?> debugInfo, final Throwable cause) {
        super(message, debugInfo, cause);
    }

    public CacheWriteException(final String message, final Object debugPojo) {
        super(message, debugPojo);
    }

    public CacheWriteException(final String message, final Object debugPojo, final Throwable cause) {
        super(message, debugPojo, cause);
    }
}