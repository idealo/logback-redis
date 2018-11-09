package de.idealo.logback.appender.jediswriter;

import static de.idealo.logback.appender.utils.ThreadUtils.createThread;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.idealo.logback.appender.jedisclient.JedisClient;

import ch.qos.logback.core.spi.DeferredProcessingAware;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

public abstract class AbstractBufferedJedisWriter implements Closeable {

    private static final int SEND_EVENT_TRIES = 2;

    /*
     * logger must not be static: logback may not be initialized, when class is loaded.
     * creating an final static field at this time may result in a null reference
     */
    private final Logger log;

    private final Function<DeferredProcessingAware, String> messageCreator;
    private final String redisKey;
    private final int maxBufferItems;
    private final long flushBufferIntervalMillis;

    private final JedisClient client;
    private final LinkedBlockingQueue<DeferredProcessingAware> bufferedEvents;
    private final Thread bufferFlusher;
    private final AtomicLong lastFlushEpochMillis;
    private final AtomicInteger flusherThreadActions = new AtomicInteger(0);
    private volatile boolean shutdown;

    AbstractBufferedJedisWriter(JedisClient client,
            Function<DeferredProcessingAware, String> messageCreator,
            String redisKey,
            int maxBufferItems,
            long flushBufferIntervalMillis) {
        log = LoggerFactory.getLogger(getClass());

        this.messageCreator = messageCreator;
        this.client = client;
        this.redisKey = redisKey;
        this.maxBufferItems = maxBufferItems;
        this.flushBufferIntervalMillis = flushBufferIntervalMillis;
        shutdown = false;
        bufferedEvents = new LinkedBlockingQueue<>();
        lastFlushEpochMillis = new AtomicLong(System.currentTimeMillis());

        bufferFlusher = createThread(this::flushPeriodically, getClass().getSimpleName(), true);
        bufferFlusher.start();
    }

    public String getRedisKey() {
        return redisKey;
    }

    public void append(DeferredProcessingAware event) {
        if (event != null && !bufferedEvents.offer(event)) {
            final String encodedEvent = messageCreator.apply(event);
            log.warn("unable to add event {} to buffer", encodedEvent);
        }
        if (maxBatchSizeReached() || maxBatchWaitTimeReached()) {
            flushBuffer();
        }
    }

    private boolean maxBatchSizeReached() {
        return bufferedEvents.size() >= maxBufferItems;
    }

    private boolean maxBatchWaitTimeReached() {
        return lastFlushEpochMillis.get() + flushBufferIntervalMillis <= System.currentTimeMillis();
    }

    private void flushBuffer() {
        try {
            final List<DeferredProcessingAware> toPush = new ArrayList<>(bufferedEvents.size());
            bufferedEvents.drainTo(toPush);
            final String[] values = toPush.stream().map(messageCreator).toArray(String[]::new);
            for (int i = 1; i <= SEND_EVENT_TRIES; i++) {
                if (sendValuesToRedis(values)) {
                    return;
                }
            }
            log.warn("unable to send events to redis: {}", Arrays.asList(values));
        } finally {
            lastFlushEpochMillis.set(System.currentTimeMillis());
        }
    }

    private boolean sendValuesToRedis(String... values) {
        if (values.length == 0) {
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
                    addValuesToPipeline(pipeline, values);
                    pipeline.sync();
                    logSendStatistics(values.length, start);
                    return true;
                }
            } catch (JedisException ex) {
                log.info("unable to send {} events, reconnecting to redis", values.length, ex);
            }
            client.reconnect();
            return false;
        }
    }

    /**
     * adds the given values to the given pipeline.<br/>
     * <br/>
     * only calls to the appropriate send-method (e.g. rpush, publish) are permitted.
     * exceptions must not be swallowed, but passed to the caller.<br/>
     * <br/>
     * an example implementation (for rpush) would be
     *
     * <pre>
     * pipeline.rpush(getRedisKey(), events);
     * </pre>
     *
     * @param pipeline
     *            pipeline that receives the event
     * @param values
     *            events to be sent to redis
     */
    abstract void addValuesToPipeline(Pipeline pipeline, String... values);

    private void logSendStatistics(int events, long startEpochMillis) {
        if (log.isDebugEnabled()) {
            long elapsedTimeMillis = System.currentTimeMillis() - startEpochMillis;
            double eventsPerMilli = Math.round(events / (double) elapsedTimeMillis);
            log.debug("sent {} events to Redis in {}ms => rate (events per milli) = {}", events, elapsedTimeMillis, eventsPerMilli);
        }
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
                final long flushWaitMillis = flushBufferIntervalMillis - (System.currentTimeMillis() - lastFlushEpochMillis.get());
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
