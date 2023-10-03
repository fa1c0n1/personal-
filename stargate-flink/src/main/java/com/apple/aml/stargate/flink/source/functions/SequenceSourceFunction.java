package com.apple.aml.stargate.flink.source.functions;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Duration;

public class SequenceSourceFunction extends RichParallelSourceFunction<Long> {
    private final long start;
    private long end = -1;
    private long current;
    private volatile boolean isRunning = true;

    private final long numElements;
    private final Duration periodLength;

    // Constructors
    public SequenceSourceFunction(long start) {
        this(start, -1l, 1, Duration.ofMillis(1)); // Default rate is 1 element per millisecond
    }

    public SequenceSourceFunction(long start, long end) {
        this(start, end, 1, Duration.ofMillis(1)); // Default rate
    }

    private SequenceSourceFunction(long start, long end, long numElements, Duration periodLength) {
        this.start = start;
        this.end = end;
        this.current = start;
        this.numElements = numElements;
        this.periodLength = periodLength;
    }

    public SequenceSourceFunction withRate(long numElements, Duration periodLength) {
        return new SequenceSourceFunction(this.start, this.end, numElements, periodLength);
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();
        while (isRunning && (end == -1l || current < end)) {
            long startTime = System.currentTimeMillis();
            synchronized (lock) {
                for (long i = 0; i < numElements && (end == -1l || current < end); i++) {
                    ctx.collect(current++);
                }
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            long sleepTime = periodLength.toMillis() - elapsedTime;

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
