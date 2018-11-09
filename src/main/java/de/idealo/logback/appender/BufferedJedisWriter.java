package de.idealo.logback.appender;

import static de.idealo.logback.appender.utils.ThreadUtils.createThread;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.DeferredProcessingAware;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

public class BufferedJedisWriter implements Closeable {

    private static final int SEND_EVENT_TRIES = 2;

    /*
     * logger must not be static: logback may not be initialized, when class is loaded.
     * creating an final static field at this time may result in a null reference
     */
    private final Logger log;

    private final Encoder<DeferredProcessingAware> encoder;
    private final String redisKey;
    private final int maxBatchItems;
    private final long maxBatchWaitMillis;

    private final JedisClient client;
    private final LinkedBlockingQueue<DeferredProcessingAware> bufferedEvents;
    private final Thread bufferFlusher;
    private final AtomicLong lastFlushEpochMillis;
    private final AtomicInteger flusherThreadActions = new AtomicInteger(0);
    private volatile boolean shutdown;

    BufferedJedisWriter(JedisClient client,
            Encoder<DeferredProcessingAware> encoder,
            String redisKey,
            int maxBatchItems,
            long maxBatchWaitMillis) {
        if (encoder == null) {
            throw new IllegalArgumentException("encoder must not be null");
        }

        log = LoggerFactory.getLogger(BufferedJedisWriter.class);
        this.client = client;
        this.encoder = encoder;
        this.redisKey = redisKey;
        this.maxBatchItems = maxBatchItems;
        this.maxBatchWaitMillis = maxBatchWaitMillis;
        shutdown = false;
        bufferedEvents = new LinkedBlockingQueue<>();
        lastFlushEpochMillis = new AtomicLong(System.currentTimeMillis());

        bufferFlusher = createThread(this::flushPeriodically, getClass().getSimpleName(), true);
        bufferFlusher.start();
    }

    public void append(DeferredProcessingAware event) {
        if (event != null && !bufferedEvents.offer(event)) {
            final String encodedEvent = createEncodedEvent(event);
            log.warn("unable to add event {} to buffer", encodedEvent);
        }
        if (maxBatchSizeReached() || maxBatchWaitTimeReached()) {
            flushBuffer();
        }
    }

    private boolean maxBatchSizeReached() {
        return bufferedEvents.size() >= maxBatchItems;
    }

    private boolean maxBatchWaitTimeReached() {
        return lastFlushEpochMillis.get() + maxBatchWaitMillis <= System.currentTimeMillis();
    }

    private void flushBuffer() {
        try {
            final List<DeferredProcessingAware> toPush = new ArrayList<>(bufferedEvents.size());
            bufferedEvents.drainTo(toPush);
            final String[] events = toPush.stream().map(this::createEncodedEvent).toArray(String[]::new);
            for (int i = 1; i <= SEND_EVENT_TRIES; i++) {
                if (pushValuesToRedis(events)) {
                    return;
                }
            }
            log.warn("unable to send events to redis: {}", Arrays.asList(events));
        } finally {
            lastFlushEpochMillis.set(System.currentTimeMillis());
        }
    }

    private boolean pushValuesToRedis(String... events) {
        if (events.length == 0) {
            return true;
        }
        synchronized (client) {
            /*
             * RedisBatchAppender-doc stated, that jedis client is not thread safe.
             * logback's AppenderBase.doAppend is synchronized, so no concurrent logs can access this method,
             * but flushing thread could be active
             */
            try {
                final Pipeline pipeline = client.getPipeline().orElse(null);
                if (pipeline != null) {
                    final long start = System.currentTimeMillis();
                    pipeline.rpush(redisKey, events);
                    pipeline.sync();
                    logPushStatistics(events.length, start);
                    return true;
                }
            } catch (JedisException ex) {
                log.info("unable to send {} events, reconnecting to redis", events.length, ex);
            }
            client.reconnect();
            return false;
        }
    }

    private void logPushStatistics(int events, long startEpochMillis) {
        if (log.isDebugEnabled()) {
            long elapsedTimeMillis = System.currentTimeMillis() - startEpochMillis;
            double eventsPerMilli = Math.round(events / (double) elapsedTimeMillis);
            log.debug("sent {} events to Redis in {}ms => rate (events per milli) = {}", events, elapsedTimeMillis, eventsPerMilli);
        }
    }

    private String createEncodedEvent(DeferredProcessingAware event) {
        return new String(encoder.encode(event), StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        log.info("closing {}", getClass().getSimpleName());
        shutdown = true;
        flushBuffer();
        client.close();
        bufferFlusher.interrupt();
    }

    int getFlusherThreadActions() {
        return flusherThreadActions.get();
    }

    @SuppressWarnings("squid:S2142")
    private void flushPeriodically() {
        while (!shutdown) {
            try {
                final long flushWaitMillis = maxBatchWaitMillis - (System.currentTimeMillis() - lastFlushEpochMillis.get());
                if (flushWaitMillis <= 0) {
                    flushBuffer();
                    flusherThreadActions.incrementAndGet();
                } else {
                    TimeUnit.MILLISECONDS.sleep(flushWaitMillis);
                }
            } catch (InterruptedException ex) {
                // ignores InterruptedException by purpose, shutdown must be set to stop it
                // setting interrupt flag would break sleep method
                log.trace("ignoring thread interruption", ex);
            } catch (Exception ex) {
                log.warn("unexpected exception occured while running flushing thread", ex);
            }
        }
    }
}
