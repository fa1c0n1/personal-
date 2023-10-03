package com.apple.aml.stargate.common.exceptions;

public class GenericRuntimeException extends RuntimeException {
    private final GenericException exception;

    public GenericRuntimeException(final GenericException exception) {
        super(exception.getMessage(), exception);
        this.exception = exception;
    }

    public GenericException getException() {
        return exception;
    }
}
