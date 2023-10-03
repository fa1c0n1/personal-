package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.options.RetryOptions;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.SneakyThrows;

import java.util.function.Function;
import java.util.function.Predicate;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialBackoff;
import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;

public class SneakyRetryFunction<T, R> implements Function<T, R> {
    final Retry retry;
    final Function<T, R> function;

    public SneakyRetryFunction(final Retry retry, final Function<T, R> function) {
        this.retry = retry;
        this.function = function;
    }

    public static <T, R> Function<T, R> applyWithRetry(final Retry retry, final Function<T, R> function) {
        return new SneakyRetryFunction<>(retry, function);
    }

    public static Retry retry(final RetryOptions options, final Predicate<Throwable> predicate) {
        if (options == null || options.getMaxAttempts() <= 0) return null;
        IntervalFunction intervalFunction;
        if (options.getMultiplier() > 0) {
            if (options.getRandomizationFactor() > 0) {
                intervalFunction = options.getMaxInterval() != null && options.getMaxInterval().toMillis() > 0 ? ofExponentialRandomBackoff(options.getInitialInterval(), options.getMultiplier(), options.getRandomizationFactor(), options.getMaxInterval()) : ofExponentialRandomBackoff(options.getInitialInterval(), options.getMultiplier(), options.getRandomizationFactor());
            } else {
                intervalFunction = options.getMaxInterval() != null && options.getMaxInterval().toMillis() > 0 ? ofExponentialBackoff(options.getInitialInterval(), options.getMultiplier(), options.getMaxInterval()) : ofExponentialBackoff(options.getInitialInterval(), options.getMultiplier());
            }
        } else {
            intervalFunction = IntervalFunction.of(options.getFixedInterval());
        }
        RetryConfig retryConfig = RetryConfig.custom().maxAttempts(options.getMaxAttempts()).intervalFunction(intervalFunction).retryOnException(predicate).build();
        return Retry.of("lambda", retryConfig);
    }

    @Override
    @SneakyThrows
    public R apply(final T t) {
        Retry.Context<R> context = retry.context();
        do {
            try {
                R result = function.apply(t);
                final boolean validationOfResult = context.onResult(result);
                if (!validationOfResult) {
                    context.onComplete();
                    return result;
                }
            } catch (Exception exception) {
                context.onError(exception);
            }
        } while (true);
    }
}
