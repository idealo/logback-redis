package de.idealo.logback.appender.utils;

import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import de.idealo.logback.appender.RedisBatchAppender;

import ch.qos.logback.classic.LoggerContext;

public final class LogbackUtils {

    private LogbackUtils() {
    }

    /**
     * Shuts down logback. This ensures that the {@link RedisBatchAppender#stop()} method is called
     * which cleans up background threads and pools and ensures that remaining messages are
     * sent to Redis before shutting down the app.
     *
     * This functionality can also be performed with a shutdown hook in logback.xml
     * (&lt;shutdownHook class="ch.qos.logback.core.hook.DelayingShutdownHook"/&gt;)
     * but this doesn't work in Spring Boot apps when shut down with the actuator shutdown URL.
     *
     * Therefore, this method should be called in Spring Boot apps as follows:
     * 
     * <pre>
     *{@literal @}Component
     * public class LogbackStopListener implements ApplicationListener&lt;ContextClosedEvent&gt; {
     *  {@literal @}Override
     *   public void onApplicationEvent(ContextClosedEvent event) {
     *     LogbackUtils.stopLogback();
     *   }
     * }
     * </pre>
     */
    public static void stopLogback() {
        LoggerFactory.getLogger(LogbackUtils.class).debug("shutting down logback");

        ILoggerFactory factory = LoggerFactory.getILoggerFactory();
        if (factory instanceof LoggerContext) {
            LoggerContext ctx = (LoggerContext) factory;
            ctx.stop();
        }
    }

}
