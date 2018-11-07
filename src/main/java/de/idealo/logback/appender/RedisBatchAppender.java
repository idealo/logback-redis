package de.idealo.logback.appender;

import java.util.concurrent.TimeUnit;

import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.DeferredProcessingAware;

/**
 * Logback appender that writes logging events in batches to redis.
 *
 * @see <a href="http://logback.qos.ch/manual/appenders.html">logback appender documentation</a>
 */
public class RedisBatchAppender extends AppenderBase<DeferredProcessingAware> {

    private static final int DEFAULT_MAX_BATCH_MESSAGES = 1000;
    private static final int DEFAULT_MAX_BATCH_SECONDS = 5;
    private static final JedisPoolFactory JEDIS_POOL_FACTORY = new JedisPoolFactory();

    // logger configurable options
    private boolean retryOnInitializeError = true;
    private int retryInitializeIntervalInSeconds = 30;
    private Encoder<DeferredProcessingAware> encoder;
    private int maxBatchMessages = DEFAULT_MAX_BATCH_MESSAGES;
    private int maxBatchSeconds = DEFAULT_MAX_BATCH_SECONDS;
    private RedisConnectionConfig connectionConfig;
    private BufferedJedisWriter writer;

    @Override
    public void start() {
        super.start();
        final JedisClientProvider clientProvider = new JedisClientProvider(JEDIS_POOL_FACTORY, connectionConfig);
        final JedisClient client = new JedisClient(clientProvider,
                retryOnInitializeError ? Integer.MAX_VALUE : 1,
                TimeUnit.SECONDS.toMillis(retryInitializeIntervalInSeconds));

        writer = new BufferedJedisWriter(client, encoder, connectionConfig.getKey(), maxBatchMessages, TimeUnit.SECONDS.toMillis(maxBatchSeconds));
    }

    @Override
    protected void append(DeferredProcessingAware event) {
        writer.append(event);
    }

    @Override
    public void stop() {
        super.stop();
        if (writer != null) {
            writer.close();
        }
    }

    public Encoder<DeferredProcessingAware> getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder<DeferredProcessingAware> encoder) {
        this.encoder = encoder;
    }

    public void setMaxBatchMessages(int maxBatchMessages) {
        this.maxBatchMessages = maxBatchMessages;
    }

    public void setMaxBatchSeconds(int maxBatchSeconds) {
        this.maxBatchSeconds = maxBatchSeconds;
    }

    public void setConnectionConfig(RedisConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public void setRetryOnInitializeError(boolean retryOnInitializeError) {
        this.retryOnInitializeError = retryOnInitializeError;
    }

    public void setRetryInitializeIntervalInSeconds(int retryInitializeIntervalInSeconds) {
        this.retryInitializeIntervalInSeconds = retryInitializeIntervalInSeconds;
    }
}
