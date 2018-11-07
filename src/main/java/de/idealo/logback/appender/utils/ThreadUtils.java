package de.idealo.logback.appender.utils;

import java.util.concurrent.atomic.AtomicLong;

public final class ThreadUtils {
    private static final AtomicLong THREAD_NUMBER = new AtomicLong(0);

    private ThreadUtils() {
        // nothing to initialize
    }

    public static Thread createThread(Runnable runnable, String threadName, boolean daemon) {
        Thread t = new Thread(runnable, threadName + "-" + THREAD_NUMBER.incrementAndGet());
        t.setDaemon(daemon);
        return t;
    }
}
