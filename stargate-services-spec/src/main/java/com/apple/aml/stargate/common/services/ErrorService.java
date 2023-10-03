package com.apple.aml.stargate.common.services;

import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public interface ErrorService {
    void publishError(final Exception exception, final Map<String, Object> object) throws Exception;

    void publishError(final Exception exception, final Map<String, Object> object, final String key) throws Exception;

    void publishError(final Exception exception, final GenericRecord record);

    void publishError(final Exception exception, final GenericRecord record, final String key);

    void publishError(final Exception exception, final Object pojo) throws Exception;

    void publishError(final Exception exception, final Object pojo, final String key) throws Exception;

}
