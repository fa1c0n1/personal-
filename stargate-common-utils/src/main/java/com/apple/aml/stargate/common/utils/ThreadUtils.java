package com.apple.aml.stargate.common.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public final class ThreadUtils {
    private ThreadUtils() {

    }

    public static ThreadFactory threadFactory(final String name, final boolean daemon) {
        return r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            if (name != null) {
                thread.setName(name);
            }
            thread.setDaemon(daemon);
            return thread;
        };
    }
}
