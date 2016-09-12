package de.idealo.logback.appender;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.DeferredProcessingAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

/**
 * Logback appender that writes logging events in batches to redis.
 *
 * This appender must be a subclass of AppenderBase (whose implementation of the doAppend() method is synchronized)
 * because the used Jedis client for Redis is not thread-safe.
 *
 * @see <a href="http://logback.qos.ch/manual/appenders.html">logback appender documentation</a>
 */
public class RedisBatchAppender extends AppenderBase<DeferredProcessingAware> {
    private final static Logger LOG = LoggerFactory.getLogger(RedisBatchAppender.class);

    private static final int    BUFFER_SIZE                = 1024 * 1024;
    private static final int    DEFAULT_MAX_BATCH_MESSAGES = 1000;
    private static final int    DEFAULT_MAX_BATCH_SECONDS  = 5;
    private static final long   REDIS_SYNC_TIMER_DELAY     = 10000L;
    private static final long   REDIS_SYNC_TIMER_PERIOD    = 10000L;
    private static final double MILLIS_PER_SECOND_DOUBLE   = 1000.0;

    private final Timer batchTimer = new Timer();
    private BatchConfig batchConfig;

    private Pool<Jedis>              pool;
    private Jedis                    client;
    private Pipeline                 pipeline;
    private ScheduledExecutorService retryExecutorService;

    // logger configurable options
    private boolean retryOnInitializeError           = true;
    private int     retryInitializeIntervalInSeconds = 30;
    private Encoder<DeferredProcessingAware> encoder;
    private int maxBatchMessages = DEFAULT_MAX_BATCH_MESSAGES;
    private int maxBatchSeconds  = DEFAULT_MAX_BATCH_SECONDS;
    private RedisConnectionConfig connectionConfig;

    private final AtomicInteger connectionStartupCounter = new AtomicInteger();
    private Instant lastLog = Instant.now();

    @Override
    public void start() {
        super.start();

        batchConfig = new BatchConfig(maxBatchMessages, maxBatchSeconds);
        checkEncoderPresent();
        initBatchScheduler();

        wrapStartupRetriesOnConnectionFailures();
    }

    private void wrapStartupRetriesOnConnectionFailures() {
        JedisPoolFactory jedisPoolFactory = new JedisPoolFactory(connectionConfig);
        try {
            startLoggingLifecycle(jedisPoolFactory);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            if (retryOnInitializeError) {
                retryExecutorService = Executors.newScheduledThreadPool(1);
                retryExecutorService.scheduleAtFixedRate(()->{
                    LOG.info("retry initializing");
                    try {
                        startLoggingLifecycle(jedisPoolFactory);
                        LOG.info("retry initializing succeded");
                        retryExecutorService.shutdown();
                        connectionStartupCounter.incrementAndGet();
                    } catch (Exception e1) {
                        LOG.error("retried initialization failed " + e1.getMessage());
                    }
                }, retryInitializeIntervalInSeconds, retryInitializeIntervalInSeconds, TimeUnit.SECONDS);
            }
        }
    }

    private void startLoggingLifecycle(JedisPoolFactory jedisPoolFactory) {
        pool = jedisPoolFactory.createPool();
        initJedisClient();
    }

    @Override
    protected void append(DeferredProcessingAware event) {
        try {
            if (pipeline == null) {
                Instant now = Instant.now();
                if (now.minus(30, SECONDS).isAfter(lastLog)) {
                    LOG.warn("pipeline not ready");
                    lastLog = now;
                }
            } else {
                LOG.debug("logging to redis: {}", String.valueOf(event));
                appendUnsafe(event);
            }
        } catch (JedisConnectionException e) {
            LOG.debug(
                    "re-create Jedis client and resend event after JedisConnectionException while appending the event '{}'.",
                    event);

            try {
                closeJedisClientGracefully();
                initJedisClient();
                appendUnsafe(event);
            } catch(Exception exceptionDuringRetry) {
                LOG.error("Exception while retrying to append the event '" + event + "' with a re-initialized Jedis client. The event is lost.",
                          exceptionDuringRetry);
            }
        } catch(Exception e) {
            LOG.error("Exception while appending the event '" + event + "'. The event is lost.", e);
        }
    }

    /**
     * Closes the jedis client gracefully. More specifically, {@link JedisException} while closing the client
     * is caught and solely logged because the client will be re-initialized afterwards.
     *
     * {@link JedisException} is thrown e.g. if the redis server is temporarily not available.
     */
    private void closeJedisClientGracefully() {
        try {
            client.close();
        } catch (JedisException e) {
            LOG.warn(
                    "Intentionally ignoring exception while closing the jedis client. The client will be re-initialized afterwards.",
                    e);
        }
    }

    /**
     * Appends an event in order to be logged. This method is unsafe concerning the fact that the event is lost
     * if the Jedis client connection has timed out due to a redis connection timeout (configured on the
     * redis server).
     *
     * @param event event to be appended
     */
    private void appendUnsafe(DeferredProcessingAware event) {
        if (event != null) {
            rpushAndSyncIfBatchFinished(event);
        } else {
            sendBatch(batchConfig.getProcessedBatchItemsCount());
        }
    }

    private void rpushAndSyncIfBatchFinished(DeferredProcessingAware event) {
        pipeline.rpush(connectionConfig.getKey(), createEncodedEvent(event));
        batchConfig.addBatchItem();

        final int batchSize = batchConfig.getProcessedBatchItemsCount();
        if (batchConfig.isBatchFinished()) {
            sendBatch(batchSize);
        }
    }

    @Override
    public void stop() {
        LOG.info("stopping");
        // pipeline must be synchronized in order to write the remaining messages to redis
        doAppend(null);
        batchTimer.cancel();

        super.stop();
        if (pool != null) {
            pool.destroy();
        }
    }

    public Encoder<DeferredProcessingAware> getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder<DeferredProcessingAware> encoder) {
        this.encoder = encoder;
    }

    public int getMaxBatchMessages() {
        return maxBatchMessages;
    }

    public void setMaxBatchMessages(int maxBatchMessages) {
        this.maxBatchMessages = maxBatchMessages;
    }

    public int getMaxBatchSeconds() {
        return maxBatchSeconds;
    }

    public void setMaxBatchSeconds(int maxBatchSeconds) {
        this.maxBatchSeconds = maxBatchSeconds;
    }

    public RedisConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(RedisConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    private void initJedisClient() {
        client = pool.getResource();
        pipeline = client.pipelined();
    }

    private void sendBatch(int batchSize) {
        if (batchSize == 0) {
            return;
        }

        long start = System.currentTimeMillis();
        pipeline.sync();
        if (LOG.isDebugEnabled()) {
            long elapsedTime = System.currentTimeMillis() - start;
            double eventsPerSecond = Math.round(MILLIS_PER_SECOND_DOUBLE * batchSize / elapsedTime);
            LOG.debug("sent {} events to Redis in {}ms => rate (events per second) = {}",
                      batchSize,
                      elapsedTime,
                      eventsPerSecond);
        }
        pipeline = client.pipelined();
    }

    private void checkEncoderPresent() {
        if (encoder == null) {
            throw new IllegalStateException("encoder must not be null");
        }
    }

    private String createEncodedEvent(DeferredProcessingAware event) {
        try (ByteArrayOutputStream eventOutputStream = new ByteArrayOutputStream(BUFFER_SIZE)) {
            encoder.init(eventOutputStream);
            encoder.doEncode(event);

            return eventOutputStream.toString("UTF-8");
        } catch (IOException e) {
            throw new IllegalStateException("error while initializing the event encoder", e);
        }
    }

    private void initBatchScheduler() {
        batchTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                doAppend(null);
            }
        }, REDIS_SYNC_TIMER_DELAY, REDIS_SYNC_TIMER_PERIOD);
    }

    public boolean isRetryOnInitializeError() {
        return retryOnInitializeError;
    }

    public void setRetryOnInitializeError(boolean retryOnInitializeError) {
        this.retryOnInitializeError = retryOnInitializeError;
    }

    public int getRetryInitializeIntervalInSeconds() {
        return retryInitializeIntervalInSeconds;
    }

    public void setRetryInitializeIntervalInSeconds(int retryInitializeIntervalInSeconds) {
        this.retryInitializeIntervalInSeconds = retryInitializeIntervalInSeconds;
    }

    //  VisibleForTesting
    int getConnectionStartupCounter() {
        return connectionStartupCounter.intValue();
    }
}
