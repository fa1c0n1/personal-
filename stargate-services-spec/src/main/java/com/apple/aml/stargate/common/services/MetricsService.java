package com.apple.aml.stargate.common.services;

import com.apple.jvm.commons.metrics.Counter;
import com.apple.jvm.commons.metrics.DoubleGauge;
import com.apple.jvm.commons.metrics.LongGauge;
import com.apple.jvm.commons.metrics.MetricsEngine;
import com.apple.jvm.commons.metrics.Timer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.ConcurrentHashMap;

public interface MetricsService extends MetricsEngine {
    ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

    default Timer timer(final String name) {
        return (Timer) map.computeIfAbsent(name, n -> newTimer(name));
    }

    default Counter counter(final String name) {
        return (Counter) map.computeIfAbsent(name, n -> newCounter(name));
    }

    default DoubleGauge doubleGauge(final String name) {
        return (DoubleGauge) map.computeIfAbsent(name, n -> newDoubleGauge(name));
    }

    default LongGauge longGauge(final String name) {
        return (LongGauge) map.computeIfAbsent(name, n -> newLongGauge(name));
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface RecordHistogram {
    }
}
