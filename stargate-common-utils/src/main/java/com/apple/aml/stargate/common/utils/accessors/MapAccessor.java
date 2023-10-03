package com.apple.aml.stargate.common.utils.accessors;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;

import java.util.Map;

public class MapAccessor implements PropertyAccessor {

    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class<?>[]{Map.class};
    }

    @Override
    public boolean canRead(final EvaluationContext context, final Object target, final String name) throws AccessException {
        return (target instanceof Map && ((Map<?, ?>) target).containsKey(name));
    }

    @Override
    public TypedValue read(final EvaluationContext context, final Object target, final String name) throws AccessException {
        Map<?, ?> map = (Map<?, ?>) target;
        Object value = map.get(name);
        if (value == null) {
            return null;
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
