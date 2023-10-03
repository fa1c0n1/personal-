package com.jsoniter.spi;

public class JsonException extends RuntimeException {
    public JsonException() {
    }

    public JsonException(final String message) {
        super(removeFullPayloadBuffer(message));
    }

    private static String removeFullPayloadBuffer(final String message) {
        int index = message.indexOf("buf");
        if (index < 0) {
            return message;
        }
        return message.substring(0, index);
    }

    public JsonException(final String message, final Throwable cause) {
        super(removeFullPayloadBuffer(message), cause);
    }

    public JsonException(final Throwable cause) {
        super(cause);
    }
}