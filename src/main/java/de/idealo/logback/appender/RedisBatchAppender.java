package de.idealo.logback.appender;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.Encoder;

/**
 * Logback appender that writes logging events in batches to redis.
 *
 * This appender must be a subclass of AppenderBase (whose implementation of the doAppend() method is synchronized)
 * because the used Jedis client for Redis is not thread-safe.
 *
 * @see <a href="http://logback.qos.ch/manual/appenders.html">logback appender documentation</a>
 */
public class RedisBatchAppender extends AppenderBase<ILoggingEvent> {

    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final int DEFAULT_MAX_BATCH_MESSAGES = 1000;
    private static final int DEFAULT_MAX_BATCH_SECONDS = 5;
    private static final long REDIS_SYNC_TIMER_DELAY = 10000L;
    private static final long REDIS_SYNC_TIMER_PERIOD = 10000L;
    private static final double MILLIS_PER_SECOND_DOUBLE = 1000.0;

    private Timer timer;
    private BatchConfig batchConfig;

    private Pool<Jedis> pool;
    private Jedis client = null;
    private Pipeline pipeline;

    // logger configurable options
    private Encoder<ILoggingEvent> encoder;
    private int maxBatchMessages = DEFAULT_MAX_BATCH_MESSAGES;
    private int maxBatchSeconds = DEFAULT_MAX_BATCH_SECONDS;
    private RedisConnectionConfig connectionConfig;

    private Object logger;

    private Logger log() {
        if (logger == null) {
            logger = LoggerFactory.getLogger(RedisBatchAppender.class);
        }
        return (Logger) logger;
    }

    @Override
    public void start() {
        super.start();

        initPool();

        batchConfig = new BatchConfig(maxBatchMessages, maxBatchSeconds);

        checkEncoderPresent();

        initJedisClient();

        initBatchScheduler();
    }

    @Override
    protected void append(ILoggingEvent event) {
        try {
            log().debug("logging to redis: " + String.valueOf(event));
            appendUnsafe(event);
        } catch(JedisConnectionException e) {
            log().debug("re-create Jedis client and resend event after JedisConnectionException while appending the event '{}'.", event);

            try {
                closeJedisClientGracefully();
                initJedisClient();
                appendUnsafe(event);
            } catch(Exception exceptionDuringRetry) {
                // exceptions during retry are logged
                log().error("Exception while retrying to append the event '" + event + "' with a re-initialized Jedis client. The event is lost.",
                        exceptionDuringRetry);
            }
        } catch(Exception e) {
            // all other exceptions during append are logged
            log().error("Exception while appending the event '" + event + "'. The event is lost.", e);
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
        } catch(JedisException e) {
            // ignore
            log().warn("Intentionally ignoring exception while closing the jedis client. The client will be re-initialized afterwards.", e);
        }
    }

    /**
     * Appends an event in order to be logged. This method is unsafe concerning the fact that the event is lost
     * if the Jedis client connection has timed out due to a redis connection timeout (configured on the
     * redis server).
     *
     * @param event event to be appended
     */
    private void appendUnsafe(ILoggingEvent event) {
        if (event != null) {
            rpushAndSyncIfBatchFinished(event);
        } else {
            sendBatch(batchConfig.getProcessedBatchItemsCount());
        }
    }

    private void rpushAndSyncIfBatchFinished(ILoggingEvent event) {
        pipeline.rpush(connectionConfig.getKey(), createEncodedEvent(event));
        batchConfig.addBatchItem();

        final int batchSize = batchConfig.getProcessedBatchItemsCount();
        if (batchConfig.isBatchFinished()) {
            sendBatch(batchSize);
        }
    }

    @Override
    public void stop() {
        // pipeline must be synchronized in order to write the remaining messages to redis
        doAppend(null);
        timer.cancel();

        super.stop();
        pool.destroy();
    }

    public Encoder<ILoggingEvent> getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder<ILoggingEvent> encoder) {
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
        if (log().isDebugEnabled()) {
            long elapsedTime = System.currentTimeMillis() - start;
            double eventsPerSecond = Math.round(MILLIS_PER_SECOND_DOUBLE * batchSize / elapsedTime);
            log().debug("sent {} events to Redis in {}ms => rate (events per second) = {}", batchSize, elapsedTime, eventsPerSecond);
        }
        pipeline = client.pipelined();
    }

    private void checkEncoderPresent() {
        if (encoder == null) {
            throw new IllegalStateException("encoder must not be null");
        }
    }

    private String createEncodedEvent(ILoggingEvent event) {
        try (ByteArrayOutputStream eventOutputStream = new ByteArrayOutputStream(BUFFER_SIZE)) {
            encoder.init(eventOutputStream);
            encoder.doEncode(event);

            return eventOutputStream.toString("UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("error while initializing the event encoder", e);
        }
    }

    private void initBatchScheduler() {
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                doAppend(null);
            }
        }, REDIS_SYNC_TIMER_DELAY, REDIS_SYNC_TIMER_PERIOD);
    }

    private void initPool() {
        pool = new JedisPoolFactory(connectionConfig).createPool();
    }
}
