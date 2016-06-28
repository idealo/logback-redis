package de.idealo.logback.appender.utils;

import java.util.concurrent.atomic.AtomicLong;

public class MDCUtils {
    private static final String SEQUENCE_NUMBER = "seq";

    private final AtomicLong sequenceNumber = new AtomicLong(0);

    public MDCUtils clear() {
        org.slf4j.MDC.clear();
        return this;
    }

    public MDCUtils initSeq() {
        putIfPresent(SEQUENCE_NUMBER, sequenceNumber.getAndIncrement());
        return this;
    }


    public MDCUtils putIfPresent(String key, String value) {
        if (value != null) {
            org.slf4j.MDC.put(key, value);
        }

        return this;
    }

    public MDCUtils putIfPresent(String key, Long value) {
        if (value != null) {
            org.slf4j.MDC.put(key, String.valueOf(value));
        }

        return this;
    }
}

