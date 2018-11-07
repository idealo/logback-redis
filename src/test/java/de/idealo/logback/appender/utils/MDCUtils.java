package de.idealo.logback.appender.utils;

public class MDCUtils {

    public MDCUtils clear() {
        org.slf4j.MDC.clear();
        return this;
    }

    public MDCUtils putIfPresent(String key, String value) {
        if (value != null) {
            org.slf4j.MDC.put(key, value);
        }
        return this;
    }

    public MDCUtils putIfPresent(String key, long value) {
        org.slf4j.MDC.put(key, Long.toString(value));
        return this;
    }
}
