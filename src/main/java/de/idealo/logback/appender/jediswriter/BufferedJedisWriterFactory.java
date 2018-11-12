package de.idealo.logback.appender.jediswriter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;

import de.idealo.logback.appender.jedisclient.JedisClient;
import de.idealo.logback.appender.jedisclient.JedisClientProvider;
import de.idealo.logback.appender.jedisclient.JedisPoolFactory;
import de.idealo.logback.appender.jedisclient.RedisConnectionConfig;
import de.idealo.logback.appender.jedisclient.RedisConnectionConfig.Method;

import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.DeferredProcessingAware;

public class BufferedJedisWriterFactory {

    private final JedisPoolFactory jedisPoolFactory;

    public BufferedJedisWriterFactory(JedisPoolFactory jedisPoolFactory) {
        this.jedisPoolFactory = jedisPoolFactory;
    }

    @SuppressWarnings("squid:S2095") // rule=resource should be closed: JedisClient is closed when writer is shut down
    public AbstractBufferedJedisWriter createJedisWriter(JedisWriterConfiguration writerConfiguration) {
        final RedisConnectionConfig connectionConfig = writerConfiguration.getConnectionConfig();
        final Method method = connectionConfig.getMethod();
        if (method == null) {
            throw getUnsupportedWriterTypeException(null);
        }
        final Encoder<DeferredProcessingAware> encoder = writerConfiguration.getEncoder();
        final Function<DeferredProcessingAware, String> messageCreator = event -> new String(encoder.encode(event), StandardCharsets.UTF_8);

        final JedisClientProvider clientProvider = new JedisClientProvider(jedisPoolFactory, writerConfiguration.getConnectionConfig());
        final JedisClient client = new JedisClient(clientProvider,
                writerConfiguration.getMaxInitializeTries(),
                writerConfiguration.getRetryInitializeIntervalMillis());

        switch (method) {
            case RPUSH:
                return new BufferedJedisRPusher(client,
                        messageCreator,
                        connectionConfig.getKey(),
                        writerConfiguration.getMaxBufferedMessages(),
                        writerConfiguration.getFlushBufferIntervalMillis());
            case PUBLISH:
                return new BufferedJedisPublisher(client,
                        messageCreator,
                        connectionConfig.getKey(),
                        writerConfiguration.getMaxBufferedMessages(),
                        writerConfiguration.getFlushBufferIntervalMillis());
            default:
                throw getUnsupportedWriterTypeException(method.name());
        }
    }

    private IllegalArgumentException getUnsupportedWriterTypeException(String type) {
        throw new IllegalArgumentException("writer type '" + type + "' is not supported, only " + Arrays.asList(Method.values()));
    }
}
