package com.apple.aml.stargate.common.utils.accessors;

import com.apple.aml.stargate.common.utils.proxies.AvroMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;

import java.util.Map;

public class GenericRecordAccessor implements PropertyAccessor {
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class<?>[]{GenericRecord.class};
    }

    @Override
    public boolean canRead(final EvaluationContext context, final Object target, final String name) throws AccessException {
        return target instanceof GenericRecord;
    }

    @Override
    public TypedValue read(final EvaluationContext context, final Object target, final String name) throws AccessException {
        Object value = null;
        try {
            value = ((GenericRecord) target).get(name);
        } catch (Exception ignored) {

        }
        if (value == null) return null;
        if (value instanceof Utf8) {
            value = value.toString();
        } else if (value instanceof Map) {
            value = new AvroMap((Map) value);
        }
        return new TypedValue(value);
    }

    @Override
    public boolean canWrite(final EvaluationContext context, final Object target, final String name) throws AccessException {
        return false;
    }

    @Override
    public void write(final EvaluationContext context, final Object target, final String name, final Object newValue) throws AccessException {

    }
}
