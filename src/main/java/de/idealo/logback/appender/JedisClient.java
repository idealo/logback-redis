package de.idealo.logback.appender;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

public class JedisClient implements Closeable {
    /*
     * logger must not be static: logback may not be initialized, when class is loaded.
     * creating an final static field at this time may result in a null reference
     */
    private final Logger log;
    private final Pool<Jedis> pool;
    private final long retryInitializeIntervalMillis;

    private Jedis client;
    private volatile boolean initializing;

    JedisClient(Pool<Jedis> pool, int maxInitFailureRetries, long retryInitializeIntervalMillis) {
        log = LoggerFactory.getLogger(JedisClient.class);
        this.pool = pool;
        this.retryInitializeIntervalMillis = retryInitializeIntervalMillis;
        initializing = true;
        initClient(maxInitFailureRetries);
    }

    public Optional<Pipeline> getPipeline() {
        return client == null ? Optional.empty() : Optional.of(client.pipelined());
    }

    public void reconnect() {
        if (initializing) {
            return;
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (JedisException ex) {
            log.warn("Intentionally ignoring exception while closing the jedis client."
                    + " The client will be re-initialized afterwards.", ex);
        }
        initClient(1);
    }

    @Override
    public void close() {
        pool.close();
    }

    private void initClient(int maxTries) {
        if (maxTries < 1) {
            throw new IllegalArgumentException("max tries must be greater than 0");
        }
        client = getValidClientOrNull();
        if (client != null || maxTries == 1) {
            initializing = false;
            return;
        }
        final ScheduledExecutorService retryExecutorService = Executors.newScheduledThreadPool(1);
        final AtomicInteger currentTry = new AtomicInteger(1);
        retryExecutorService.scheduleAtFixedRate(
                () -> retryConnect(retryExecutorService, currentTry, maxTries),
                retryInitializeIntervalMillis,
                retryInitializeIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    private void retryConnect(ExecutorService retryExecutorService, AtomicInteger currentTry, int maxTries) {
        currentTry.incrementAndGet();
        log.info("connect retry {}", currentTry);
        client = getValidClientOrNull();
        if (client != null || currentTry.get() >= maxTries) {
            initializing = false;
            retryExecutorService.shutdown();
        }
    }

    private Jedis getValidClientOrNull() {
        try {
            return pool.getResource();
        } catch (Exception ex) {
            log.warn("unable to get client from pool", ex);
            return null;
        }
    }
}
